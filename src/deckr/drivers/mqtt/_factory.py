from __future__ import annotations

import json
import logging
import uuid
from collections import defaultdict
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any, Literal

import aiomqtt
import anyio
import yaml
from deckr.components import (
    BaseComponent,
    ComponentContext,
    ComponentDefinition,
    ComponentManifest,
    RunContext,
)
from deckr.contracts.messages import (
    DeckrMessage,
    EndpointAddress,
    EndpointTarget,
    endpoint_target,
    hardware_manager_address,
)
from deckr.hardware import messages as hw_messages
from deckr.lanes import EndpointLane, Lane
from deckr.state import (
    DeviceClaim,
    EndpointPresence,
    HardwareInventory,
    HardwareInventoryDevice,
    StateConflict,
    StateStore,
    StateUnavailable,
    encode_key_token,
    hardware_inventory_key,
    parse_device_claim_key,
    parse_presence_endpoint_key,
    presence_endpoint_key,
)
from decouple import config as decouple_config
from pydantic import BaseModel, Field, field_validator
from watchfiles import Change, awatch

from ._device import RemoteDevice

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(decouple_config("CONFIG_DIR", default="settings")).resolve()
QOS = 2
PRESENCE_HEARTBEAT_SECONDS = 5.0
PRESENCE_TTL_SECONDS = 15
_STATE_RECONCILE_SECONDS = 1.0
_WATCH_RETRY_SECONDS = 1.0
_CONTROLLER_PRESENCE_PREFIX = ".".join(
    (
        "presence",
        "endpoint",
        encode_key_token("hardware_messages"),
        encode_key_token("controller"),
        "",
    )
)

RemoteGesture = Literal[
    "key_down",
    "key_up",
    "encoder_rotate",
    "touch_tap",
    "touch_swipe",
]


class RemoteMqttConfig(BaseModel):
    hostname: str | None = None
    port: int | None = None
    username: str | None = None
    password: str | None = None
    topic: str
    dedupe_ms: int = 250


class DriverBrokerConfig(BaseModel):
    hostname: str = ""
    port: int = 1883
    username: str | None = None
    password: str | None = None


class DriverConfig(BaseModel):
    manager_id: str
    config_path: Path | None = None
    broker: DriverBrokerConfig = Field(default_factory=DriverBrokerConfig)


class RemoteEventMapping(BaseModel):
    match: str
    slot: str
    gesture: RemoteGesture
    direction: Literal["clockwise", "counterclockwise", "left", "right"] | None = None

    @field_validator("match", mode="before")
    @classmethod
    def _normalize_match(cls, value: Any) -> str:
        # YAML treats unquoted "on"/"off" as booleans, which is surprising for
        # common MQTT action values. Normalize those back to the expected strings.
        if value is True:
            return "on"
        if value is False:
            return "off"
        return str(value)


class RemoteConfig(BaseModel):
    mqtt: RemoteMqttConfig
    events: list[RemoteEventMapping] = Field(default_factory=list)


class RemoteDeviceCandidate(BaseModel):
    id: str
    name: str
    remote: RemoteConfig


@dataclass(frozen=True, slots=True)
class MqttBrokerDefaults:
    hostname: str
    port: int
    username: str | None
    password: str | None


@dataclass(frozen=True, slots=True)
class RuntimeRemoteMapping:
    match: str
    slot: str
    gesture: RemoteGesture
    direction: Literal["clockwise", "counterclockwise", "left", "right"] | None = None

    def to_hardware_input(self) -> hw_messages.HardwareInputMessage:
        if self.gesture == "key_down":
            return hw_messages.KeyDownMessage(key_id=self.slot)
        if self.gesture == "key_up":
            return hw_messages.KeyUpMessage(key_id=self.slot)
        if self.gesture == "encoder_rotate":
            direction = self.direction
            if direction not in {"clockwise", "counterclockwise"}:
                raise ValueError(
                    f"encoder_rotate mapping for {self.slot!r} requires direction"
                )
            return hw_messages.DialRotateMessage(
                dial_id=self.slot,
                direction=direction,
            )
        if self.gesture == "touch_tap":
            return hw_messages.TouchTapMessage(touch_id=self.slot)
        if self.gesture == "touch_swipe":
            direction = self.direction
            if direction not in {"left", "right"}:
                raise ValueError(
                    f"touch_swipe mapping for {self.slot!r} requires direction"
                )
            return hw_messages.TouchSwipeMessage(
                touch_id=self.slot,
                direction=direction,
            )
        raise ValueError(f"Unsupported remote gesture: {self.gesture}")


@dataclass(frozen=True, slots=True)
class RemoteDeviceRuntime:
    id: str
    name: str
    mqtt_hostname: str
    mqtt_port: int
    mqtt_username: str | None
    mqtt_password: str | None
    mqtt_topic: str
    dedupe_ms: int
    mappings: tuple[RuntimeRemoteMapping, ...]


@dataclass(slots=True)
class RunningRemoteDevice:
    runtime: RemoteDeviceRuntime
    cancel_scope: anyio.CancelScope
    stopped: anyio.Event


@dataclass(frozen=True, slots=True)
class ResetDeviceCommand:
    pass


DeviceCommand = DeckrMessage | ResetDeviceCommand
InputPublisher = Callable[[DeckrMessage], Awaitable[None]]


def _parse_coordinates(slot_id: str) -> hw_messages.HardwareCoordinates:
    parts = slot_id.split(",")
    if len(parts) == 2 and all(part.strip("-").isdigit() for part in parts):
        return hw_messages.HardwareCoordinates(column=int(parts[0]), row=int(parts[1]))
    return hw_messages.HardwareCoordinates(column=0, row=0)


def _slot_type_for_gestures(gestures: set[RemoteGesture]) -> str:
    if "encoder_rotate" in gestures:
        return "encoder"
    if any(gesture.startswith("touch_") for gesture in gestures):
        return "touch_strip"
    return "button"


def build_slots(mappings: list[RemoteEventMapping]) -> list[hw_messages.HardwareSlot]:
    by_slot: dict[str, set[RemoteGesture]] = defaultdict(set)
    for mapping in mappings:
        by_slot[mapping.slot].add(mapping.gesture)
    return [
        hw_messages.HardwareSlot(
            id=slot_id,
            coordinates=_parse_coordinates(slot_id),
            image_format=None,
            slot_type=_slot_type_for_gestures(gestures),
            gestures=sorted(gestures),
        )
        for slot_id, gestures in sorted(
            by_slot.items(),
            key=lambda item: (
                _parse_coordinates(item[0]).row,
                _parse_coordinates(item[0]).column,
                item[0],
            ),
        )
    ]


def _extract_action_values(payload: bytes | str) -> list[str]:
    if isinstance(payload, bytes):
        raw = payload.decode("utf-8").strip()
    else:
        raw = payload.strip()

    if not raw:
        return []

    values: list[str] = [raw]
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return values

    if isinstance(data, str):
        values.append(data)
    elif isinstance(data, dict):
        action = data.get("action")
        if isinstance(action, str) and action:
            values.append(action)

    # Preserve order while dropping duplicates.
    return list(dict.fromkeys(values))


def load_mqtt_broker_defaults(
    config: Mapping[str, Any] | None = None,
) -> MqttBrokerDefaults:
    """Defaults for remote MQTT broker fields."""
    if config is not None:
        driver_config = DriverBrokerConfig.model_validate(dict(config.get("broker") or {}))
        return MqttBrokerDefaults(
            hostname=driver_config.hostname,
            port=driver_config.port,
            username=driver_config.username,
            password=driver_config.password,
        )

    hostname = decouple_config("MQTT_HOSTNAME", default="").strip()
    port = decouple_config("MQTT_PORT", default=1883, cast=int)
    username = decouple_config("MQTT_USERNAME", default="").strip() or None
    password = (
        decouple_config("MQTT_PASSWORD", default="").strip() if username else None
    )
    return MqttBrokerDefaults(
        hostname=hostname,
        port=port,
        username=username,
        password=password,
    )


def load_driver_config(config: Mapping[str, Any] | None = None) -> DriverConfig:
    driver_config = DriverConfig.model_validate(dict(config or {}))
    config_path = driver_config.config_path or CONFIG_DIR
    driver_config.config_path = Path(config_path).expanduser().resolve()
    return driver_config


class Deduper:
    def __init__(self, dedupe_ms: int):
        self._dedupe_ms = dedupe_ms
        self._last_seen_at: dict[str, float] = {}

    def should_emit(self, key: str) -> bool:
        now = monotonic()
        last_seen = self._last_seen_at.get(key)
        self._last_seen_at[key] = now
        if last_seen is None:
            return True
        return (now - last_seen) * 1000 >= self._dedupe_ms


def _load_remote_device(
    path: Path,
    *,
    default_mqtt: MqttBrokerDefaults,
) -> RemoteDeviceRuntime | None:
    try:
        data = yaml.safe_load(path.read_text())
    except Exception:
        logger.exception("Failed to read remote config from %s", path)
        return None

    if not isinstance(data, dict) or "remote" not in data:
        return None

    try:
        candidate = RemoteDeviceCandidate.model_validate(data)
    except Exception:
        logger.exception("Invalid remote device config in %s", path)
        return None

    mappings = tuple(
        RuntimeRemoteMapping(
            match=mapping.match,
            slot=mapping.slot,
            gesture=mapping.gesture,
            direction=mapping.direction,
        )
        for mapping in candidate.remote.events
    )
    if not mappings:
        logger.warning("Skipping remote config %s because it has no events", path)
        return None

    hostname = (
        candidate.remote.mqtt.hostname.strip()
        if candidate.remote.mqtt.hostname is not None
        else default_mqtt.hostname
    )
    port = (
        candidate.remote.mqtt.port
        if candidate.remote.mqtt.port is not None
        else default_mqtt.port
    )
    username = (
        candidate.remote.mqtt.username
        if candidate.remote.mqtt.username is not None
        else default_mqtt.username
    )
    password = (
        candidate.remote.mqtt.password
        if candidate.remote.mqtt.password is not None
        else default_mqtt.password
    )
    if not hostname:
        logger.warning(
            "Skipping remote config %s because no MQTT hostname is configured",
            path,
        )
        return None

    return RemoteDeviceRuntime(
        id=candidate.id,
        name=candidate.name,
        mqtt_hostname=hostname,
        mqtt_port=port,
        mqtt_username=username,
        mqtt_password=password,
        mqtt_topic=candidate.remote.mqtt.topic,
        dedupe_ms=max(candidate.remote.mqtt.dedupe_ms, 0),
        mappings=mappings,
    )


def load_remote_devices(
    config_dir: Path = CONFIG_DIR,
    *,
    default_mqtt: MqttBrokerDefaults | None = None,
) -> list[RemoteDeviceRuntime]:
    devices: list[RemoteDeviceRuntime] = []
    mqtt_defaults = default_mqtt or load_mqtt_broker_defaults()
    for pattern in ("*.yml", "*.yaml"):
        for path in sorted(config_dir.glob(pattern)):
            device = _load_remote_device(path, default_mqtt=mqtt_defaults)
            if device is not None:
                devices.append(device)
    return devices


def _yaml_filter(change: Change, path: str) -> bool:
    return path.endswith(".yml") or path.endswith(".yaml")


async def _forward_device_events(
    device: RemoteDevice,
    publish_input: InputPublisher,
    manager_id: str,
) -> None:
    async for event in device.subscribe():
        await publish_input(
            hw_messages.hardware_input_message(
                manager_id=manager_id,
                device_id=device.id,
                body=event,
            )
        )


async def _run_until_complete(cancel_scope, func, *args) -> None:
    try:
        await func(*args)
    finally:
        cancel_scope.cancel()


async def _apply_device_commands(
    device: RemoteDevice,
    command_stream: anyio.abc.ObjectReceiveStream[DeviceCommand],
    manager_id: str,
) -> None:
    async for command in command_stream:
        if isinstance(command, ResetDeviceCommand):
            for slot in device.slots:
                await device.clear_slot(slot.id)
            continue
        envelope = command
        ref = hw_messages.hardware_device_ref_from_message(envelope)
        if ref != hw_messages.HardwareDeviceRef(
            manager_id=manager_id,
            device_id=device.id,
        ):
            continue
        message = hw_messages.hardware_body_from_message(envelope)
        if not isinstance(message, hw_messages.HARDWARE_COMMAND_MESSAGE_TYPES):
            continue
        if isinstance(message, hw_messages.SetImageMessage):
            await device.set_image(message.slot_id, message.image)
        elif isinstance(message, hw_messages.ClearSlotMessage):
            await device.clear_slot(message.slot_id)
        elif isinstance(message, hw_messages.SleepScreenMessage):
            await device.sleep_screen()
        elif isinstance(message, hw_messages.WakeScreenMessage):
            await device.wake_screen()


async def _mqtt_loop(
    runtime: RemoteDeviceRuntime,
    device: RemoteDevice,
    *,
    hostname: str,
    port: int,
    username: str | None,
    password: str | None,
) -> None:
    backoff = 1.0
    deduper = Deduper(runtime.dedupe_ms)
    mappings_by_value: dict[str, list[RuntimeRemoteMapping]] = defaultdict(list)
    for mapping in runtime.mappings:
        mappings_by_value[mapping.match].append(mapping)

    cancelled_exc = anyio.get_cancelled_exc_class()
    while True:
        try:
            async with aiomqtt.Client(
                hostname,
                port=port,
                username=username,
                password=password,
            ) as client:
                await client.subscribe(runtime.mqtt_topic, qos=QOS)
                logger.info(
                    "Remote device %s subscribed to MQTT topic %s",
                    runtime.id,
                    runtime.mqtt_topic,
                )
                backoff = 1.0
                async for message in client.messages:
                    values = _extract_action_values(message.payload)
                    for value in values:
                        matched = mappings_by_value.get(value, [])
                        if not matched:
                            continue
                        if not deduper.should_emit(value):
                            continue
                        for mapping in matched:
                            await device.emit(mapping.to_hardware_input())
        except cancelled_exc:
            raise
        except Exception:
            logger.exception(
                "Remote device %s disconnected from MQTT; retrying in %.1fs",
                runtime.id,
                backoff,
            )
            await anyio.sleep(backoff)
            backoff = min(backoff * 2.0, 10.0)


async def device_loop(
    runtime: RemoteDeviceRuntime,
    publish_input: InputPublisher,
    command_stream: anyio.abc.ObjectReceiveStream[DeviceCommand],
    manager_id: str,
) -> None:
    device = RemoteDevice(
        device_id=runtime.id,
        name=runtime.name,
        slots=build_slots(
            [
                RemoteEventMapping(
                    match=mapping.match,
                    slot=mapping.slot,
                    gesture=mapping.gesture,
                    direction=mapping.direction,
                )
                for mapping in runtime.mappings
            ]
        ),
    )

    try:

        async def run_mqtt_loop() -> None:
            await _mqtt_loop(
                runtime,
                device,
                hostname=runtime.mqtt_hostname,
                port=runtime.mqtt_port,
                username=runtime.mqtt_username,
                password=runtime.mqtt_password,
            )

        async with anyio.create_task_group() as tg:
            tg.start_soon(
                _run_until_complete,
                tg.cancel_scope,
                _forward_device_events,
                device,
                publish_input,
                manager_id,
            )
            tg.start_soon(
                _run_until_complete,
                tg.cancel_scope,
                _apply_device_commands,
                device,
                command_stream,
                manager_id,
            )
            tg.start_soon(_run_until_complete, tg.cancel_scope, run_mqtt_loop)
    finally:
        await device.close()


class RemoteDeviceFactoryComponent(BaseComponent):
    def __init__(
        self,
        hardware_lane: Lane,
        state: StateStore,
        *,
        manager_id: str,
        config_dir: Path = CONFIG_DIR,
        default_mqtt: MqttBrokerDefaults | None = None,
    ):
        super().__init__(name="remote_device_factory")
        self._hardware_lane = hardware_lane
        self._state = state
        self._manager_id = manager_id
        self._config_dir = config_dir
        self._default_mqtt = default_mqtt or load_mqtt_broker_defaults()
        self._session_id = str(uuid.uuid4())
        self._cancel_scope: anyio.CancelScope | None = None
        self._endpoint: EndpointLane | None = None
        self._task_group: anyio.abc.TaskGroup | None = None
        self._stop_event: anyio.Event | None = None
        self._running_devices: dict[str, RunningRemoteDevice] = {}
        self._devices: dict[str, hw_messages.HardwareDevice] = {}
        self._claims: dict[str, DeviceClaim] = {}
        self._controller_presence_sessions: dict[EndpointAddress, str] = {}
        self._unroutable_devices: set[str] = set()
        self._command_streams: dict[str, anyio.abc.ObjectSendStream[DeviceCommand]] = {}
        self._presence_revision: int | None = None
        self._inventory_revision: int | None = None
        self._routing_reconcile_lock = anyio.Lock()

    async def start(self, ctx: RunContext) -> None:
        self._endpoint = self._hardware_lane.endpoint(
            hardware_manager_address(self._manager_id)
        )
        self._cancel_scope = ctx.tg.cancel_scope
        self._task_group = ctx.tg
        self._stop_event = anyio.Event()
        await self._reconcile_devices()
        ctx.tg.start_soon(self._presence_loop)
        ctx.tg.start_soon(self._command_subscription_loop)
        ctx.tg.start_soon(self._claim_watch_loop)
        ctx.tg.start_soon(self._controller_presence_loop)
        ctx.tg.start_soon(self._routing_reconciliation_loop)
        ctx.tg.start_soon(self._watch_loop)

    async def stop(self) -> None:
        with anyio.CancelScope(shield=True):
            if self._stop_event is not None:
                self._stop_event.set()
            if self._cancel_scope is not None:
                self._cancel_scope.cancel()
            self._devices.clear()
            self._claims.clear()
            self._unroutable_devices.clear()
            await self._withdraw_presence()
            await self._withdraw_inventory()

    async def _run_device(self, runtime: RemoteDeviceRuntime) -> None:
        stopped = anyio.Event()
        command_send, command_receive = anyio.create_memory_object_stream[
            DeviceCommand
        ](max_buffer_size=100)
        with anyio.CancelScope() as scope:
            self._running_devices[runtime.id] = RunningRemoteDevice(
                runtime=runtime,
                cancel_scope=scope,
                stopped=stopped,
            )
            self._command_streams[runtime.id] = command_send
            try:
                async with command_send, command_receive:
                    await device_loop(
                        runtime,
                        self._handle_device_message,
                        command_receive,
                        self._manager_id,
                    )
            finally:
                self._command_streams.pop(runtime.id, None)
                stopped.set()
                current = self._running_devices.get(runtime.id)
                if current is not None and current.stopped is stopped:
                    del self._running_devices[runtime.id]

    async def _stop_device(self, device_id: str) -> None:
        running = self._running_devices.get(device_id)
        if running is None:
            return
        running.cancel_scope.cancel()
        await running.stopped.wait()

    async def _start_device(self, runtime: RemoteDeviceRuntime) -> None:
        if self._task_group is None:
            return
        self._task_group.start_soon(self._run_device, runtime)

    async def _reconcile_devices(self) -> None:
        desired = {
            runtime.id: runtime
            for runtime in load_remote_devices(
                self._config_dir,
                default_mqtt=self._default_mqtt,
            )
        }
        running_ids = set(self._running_devices)
        desired_ids = set(desired)
        changed_ids = {
            device_id
            for device_id, runtime in desired.items()
            if self._running_devices.get(device_id) is not None
            and self._running_devices[device_id].runtime != runtime
        }

        for device_id in sorted((running_ids - desired_ids) | changed_ids):
            await self._stop_device(device_id)

        next_devices = {
            device_id: _hardware_device_from_runtime(runtime)
            for device_id, runtime in desired.items()
        }
        if next_devices != self._devices:
            removed_devices = set(self._devices) - set(next_devices)
            self._devices = next_devices
            for device_id in removed_devices:
                self._claims.pop(device_id, None)
                self._unroutable_devices.discard(device_id)
            await self._publish_inventory_safely()

        for device_id, runtime in desired.items():
            current = self._running_devices.get(device_id)
            if current is None:
                await self._start_device(runtime)
                continue

    async def _watch_loop(self) -> None:
        if self._stop_event is None:
            return
        try:
            async for _changes in awatch(
                self._config_dir,
                watch_filter=_yaml_filter,
                recursive=False,
                stop_event=self._stop_event,
            ):
                await self._reconcile_devices()
        except anyio.get_cancelled_exc_class():
            raise
        except Exception:
            logger.exception("Remote device config watch loop failed")

    async def _presence_loop(self) -> None:
        if self._endpoint is None:
            return
        key = presence_endpoint_key(
            lane=self._endpoint.lane.name,
            endpoint=self._endpoint.endpoint,
        )
        while True:
            try:
                entry = await self._state.put(
                    key,
                    EndpointPresence(
                        endpoint=self._endpoint.endpoint,
                        lane=self._endpoint.lane.name,
                        sessionId=self._session_id,
                        timestamp=datetime.now(UTC),
                        ttlSeconds=PRESENCE_TTL_SECONDS,
                        metadata={"runtime": "deckr-driver-mqtt-python"},
                    ),
                    ttl=PRESENCE_TTL_SECONDS,
                )
                self._presence_revision = entry.revision
                await self._publish_inventory_safely()
            except StateUnavailable:
                logger.warning(
                    "MQTT manager current state is unavailable; heartbeat will retry",
                    exc_info=True,
                )
            await anyio.sleep(PRESENCE_HEARTBEAT_SECONDS)

    async def _withdraw_presence(self) -> None:
        if self._endpoint is None:
            return
        revision = self._presence_revision
        if revision is None:
            return
        key = presence_endpoint_key(
            lane=self._endpoint.lane.name,
            endpoint=self._endpoint.endpoint,
        )
        with anyio.CancelScope(shield=True):
            try:
                await self._state.delete(key, revision=revision)
                self._presence_revision = None
            except StateConflict:
                logger.debug("MQTT manager presence changed before withdrawal")
            except StateUnavailable:
                logger.warning("Failed to withdraw MQTT manager presence", exc_info=True)

    async def _handle_device_message(self, message: DeckrMessage) -> None:
        if self._endpoint is None:
            return
        event = hw_messages.hardware_body_from_message(message)
        ref = hw_messages.hardware_device_ref_from_message(message)
        if ref is None:
            return
        if not isinstance(event, hw_messages.HARDWARE_INPUT_MESSAGE_TYPES):
            return
        if isinstance(
            event,
            hw_messages.DeviceConnectedMessage | hw_messages.DeviceDisconnectedMessage,
        ):
            return
        if ref.device_id not in self._devices:
            logger.debug("Dropping input for unknown MQTT device %s", ref.device_id)
            return
        recipient = self._claim_recipient(ref.device_id)
        if recipient is None:
            logger.debug(
                "Dropping unclaimed MQTT input for %s/%s",
                ref.manager_id,
                ref.device_id,
            )
            return
        await self._endpoint.publish(
            hw_messages.hardware_message(
                sender=self._endpoint.endpoint,
                recipient=endpoint_target(recipient),
                message_type=message.message_type,
                body=event,
                subject=message.subject,
                causation_id=message.causation_id,
            )
        )

    async def _publish_inventory(self) -> None:
        if self._endpoint is None:
            return
        entry = await self._state.put(
            hardware_inventory_key(self._manager_id),
            HardwareInventory(
                managerId=self._manager_id,
                managerEndpoint=self._endpoint.endpoint,
                sessionId=self._session_id,
                timestamp=datetime.now(UTC),
                ttlSeconds=PRESENCE_TTL_SECONDS,
                devices={
                    device_id: HardwareInventoryDevice(
                        deviceId=device_id,
                        hardwareType=device.name or "mqtt-remote",
                        fingerprint=device.fingerprint,
                        descriptor=device.model_dump(
                            by_alias=True,
                            exclude_none=True,
                            mode="json",
                        ),
                    )
                    for device_id, device in sorted(self._devices.items())
                },
            ),
            ttl=PRESENCE_TTL_SECONDS,
        )
        self._inventory_revision = entry.revision

    async def _publish_inventory_safely(self) -> None:
        try:
            await self._publish_inventory()
        except StateUnavailable:
            logger.warning(
                "MQTT inventory current state is unavailable; heartbeat will retry",
                exc_info=True,
            )

    async def _withdraw_inventory(self) -> None:
        revision = self._inventory_revision
        if revision is None:
            return
        with anyio.CancelScope(shield=True):
            try:
                await self._state.delete(
                    hardware_inventory_key(self._manager_id),
                    revision=revision,
                )
                self._inventory_revision = None
            except StateConflict:
                logger.debug("MQTT inventory changed before withdrawal")
            except StateUnavailable:
                logger.warning("Failed to withdraw MQTT inventory", exc_info=True)

    async def _claim_watch_loop(self) -> None:
        prefix = f"claim.device.{encode_key_token(self._manager_id)}."
        while True:
            try:
                async with self._state.watch(prefix) as stream:
                    async for change in stream:
                        parsed = parse_device_claim_key(change.key)
                        if parsed is None:
                            continue
                        manager_id, _device_id = parsed
                        if manager_id != self._manager_id:
                            continue
                        await self._reconcile_routing_current_state(
                            reason="device claim watch"
                        )
            except StateUnavailable:
                logger.warning(
                    "MQTT device claim state is unavailable; watch will retry",
                    exc_info=True,
                )
                await anyio.sleep(_WATCH_RETRY_SECONDS)

    async def _controller_presence_loop(self) -> None:
        while True:
            try:
                async with self._state.watch(_CONTROLLER_PRESENCE_PREFIX) as stream:
                    async for change in stream:
                        parsed = parse_presence_endpoint_key(change.key)
                        if parsed is None:
                            continue
                        lane, endpoint = parsed
                        if lane != "hardware_messages" or endpoint.family != "controller":
                            continue
                        await self._reconcile_routing_current_state(
                            reason="controller presence watch"
                        )
            except StateUnavailable:
                logger.warning(
                    "Controller endpoint presence state is unavailable; watch will retry",
                    exc_info=True,
                )
                await anyio.sleep(_WATCH_RETRY_SECONDS)

    async def _routing_reconciliation_loop(self) -> None:
        while True:
            try:
                await self._reconcile_routing_current_state(reason="broker snapshot")
            except StateUnavailable:
                logger.warning(
                    "MQTT routing current state unavailable; reconciliation will retry",
                    exc_info=True,
                )
            await anyio.sleep(_STATE_RECONCILE_SECONDS)

    async def _reconcile_routing_current_state(self, *, reason: str) -> None:
        async with self._routing_reconcile_lock:
            await self._reconcile_routing_current_state_locked(reason=reason)

    async def _reconcile_routing_current_state_locked(self, *, reason: str) -> None:
        claim_prefix = f"claim.device.{encode_key_token(self._manager_id)}."
        claim_entries = await self._state.items(claim_prefix)
        presence_entries = await self._state.items(_CONTROLLER_PRESENCE_PREFIX)

        next_claims: dict[str, DeviceClaim] = {}
        invalid_claim_devices: set[str] = set()
        next_controller_sessions: dict[EndpointAddress, str] = {}

        for entry in claim_entries:
            parsed = parse_device_claim_key(entry.key)
            if parsed is None:
                continue
            manager_id, device_id = parsed
            if manager_id != self._manager_id:
                continue
            try:
                next_claims[device_id] = DeviceClaim.model_validate(entry.value)
            except ValueError:
                logger.warning("Ignoring invalid MQTT device claim %s", entry.key)
                invalid_claim_devices.add(device_id)

        for entry in presence_entries:
            parsed = parse_presence_endpoint_key(entry.key)
            if parsed is None:
                continue
            lane, endpoint = parsed
            if lane != "hardware_messages" or endpoint.family != "controller":
                continue
            try:
                presence = EndpointPresence.model_validate(entry.value)
            except ValueError:
                logger.warning("Ignoring invalid controller presence %s", entry.key)
                continue
            if presence.endpoint != endpoint or presence.lane != lane:
                logger.warning(
                    "Ignoring controller presence %s with mismatched payload",
                    entry.key,
                )
                continue
            next_controller_sessions[endpoint] = presence.session_id

        logger.debug("Reconciling MQTT routing current state via %s", reason)
        devices_to_reset = self._devices_to_reset_for_routing_snapshot(
            next_claims,
            next_controller_sessions,
            invalid_claim_devices,
        )
        self._claims = next_claims
        self._controller_presence_sessions = next_controller_sessions
        self._unroutable_devices = {
            device_id
            for device_id, claim in next_claims.items()
            if _claim_recipient(claim, next_controller_sessions) is None
        }
        for device_id in sorted(devices_to_reset):
            await self._reset_device(device_id)

    def _devices_to_reset_for_routing_snapshot(
        self,
        next_claims: dict[str, DeviceClaim],
        next_controller_sessions: dict[EndpointAddress, str],
        invalid_claim_devices: set[str],
    ) -> set[str]:
        devices_to_reset = set(invalid_claim_devices)
        for device_id, old_claim in self._claims.items():
            next_claim = next_claims.get(device_id)
            if next_claim is None:
                devices_to_reset.add(device_id)
                continue
            if _claim_route_identity(old_claim) != _claim_route_identity(next_claim):
                devices_to_reset.add(device_id)
                continue
            if (
                _claim_recipient(old_claim, self._controller_presence_sessions)
                is not None
                and _claim_recipient(next_claim, next_controller_sessions) is None
            ):
                devices_to_reset.add(device_id)

        for device_id, next_claim in next_claims.items():
            if (
                device_id not in self._claims
                and _claim_recipient(next_claim, next_controller_sessions) is None
            ):
                devices_to_reset.add(device_id)
        return devices_to_reset

    def _claim_recipient(self, device_id: str) -> EndpointAddress | None:
        claim = self._claims.get(device_id)
        if claim is None:
            return None
        return _claim_recipient(claim, self._controller_presence_sessions)

    async def _reset_device(self, device_id: str) -> None:
        stream = self._command_streams.get(device_id)
        if stream is None:
            return
        try:
            await stream.send(ResetDeviceCommand())
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            logger.debug("Could not reset closed MQTT device session %s", device_id)

    async def _command_subscription_loop(self) -> None:
        if self._endpoint is None:
            return
        async with self._endpoint.subscribe() as stream:
            async for envelope in stream:
                await self._route_command(envelope)

    async def _route_command(self, envelope: DeckrMessage) -> None:
        if self._endpoint is None:
            return
        if (
            not isinstance(envelope.recipient, EndpointTarget)
            or envelope.recipient.endpoint != self._endpoint.endpoint
        ):
            return
        ref = hw_messages.hardware_device_ref_from_message(envelope)
        if ref is None or ref.manager_id != self._manager_id:
            return
        message = hw_messages.hardware_body_from_message(envelope)
        if not isinstance(message, hw_messages.HARDWARE_COMMAND_MESSAGE_TYPES):
            return
        if ref.device_id not in self._devices:
            logger.debug(
                "Dropping command for unknown MQTT device %s/%s",
                ref.manager_id,
                ref.device_id,
            )
            return
        if self._claim_recipient(ref.device_id) != envelope.sender:
            logger.debug(
                "Dropping unroutable MQTT command for %s/%s from %s",
                ref.manager_id,
                ref.device_id,
                envelope.sender,
            )
            return
        command_stream = self._command_streams.get(ref.device_id)
        if command_stream is None:
            logger.debug(
                "Dropping command for closed MQTT device %s/%s",
                ref.manager_id,
                ref.device_id,
            )
            return
        await command_stream.send(envelope)


def _hardware_device_from_runtime(
    runtime: RemoteDeviceRuntime,
) -> hw_messages.HardwareDevice:
    slots = build_slots(
        [
            RemoteEventMapping(
                match=mapping.match,
                slot=mapping.slot,
                gesture=mapping.gesture,
                direction=mapping.direction,
            )
            for mapping in runtime.mappings
        ]
    )
    return hw_messages.HardwareDevice(
        id=runtime.id,
        fingerprint=runtime.id,
        hid=f"remote:{runtime.id}",
        slots=slots,
        name=runtime.name,
    )


def _claim_route_identity(claim: DeviceClaim) -> tuple[EndpointAddress, str]:
    return claim.claimed_by_endpoint, claim.claimed_by_session_id


def _claim_recipient(
    claim: DeviceClaim,
    controller_presence_sessions: dict[EndpointAddress, str],
) -> EndpointAddress | None:
    session_id = controller_presence_sessions.get(claim.claimed_by_endpoint)
    if session_id != claim.claimed_by_session_id:
        return None
    return claim.claimed_by_endpoint


def driver_factory(
    hardware_lane: Lane,
    state: StateStore,
    config: Mapping[str, Any] | None = None,
):
    driver_config = load_driver_config(config)
    return RemoteDeviceFactoryComponent(
        hardware_lane,
        state,
        manager_id=driver_config.manager_id,
        config_dir=driver_config.config_path or CONFIG_DIR,
        default_mqtt=load_mqtt_broker_defaults(config),
    )


def component_factory(context: ComponentContext):
    return driver_factory(
        context.require_lane("hardware_messages"),
        context.state(),
        config=context.raw_config,
    )


component = ComponentDefinition(
    manifest=ComponentManifest(
        component_id="deckr.drivers.mqtt",
        config_prefix="deckr.drivers.mqtt",
        consumes=("hardware_messages",),
        publishes=("hardware_messages",),
    ),
    factory=component_factory,
)
