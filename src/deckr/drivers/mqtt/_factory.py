from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Any, Literal

import aiomqtt
import anyio
import yaml
from deckr.core.component import BaseComponent, RunContext
from deckr.core.components import (
    ComponentContext,
    ComponentDefinition,
    ComponentManifest,
)
from deckr.hardware import events as hw_events
from deckr.transports.bus import EventBus
from decouple import config as decouple_config
from pydantic import BaseModel, Field, field_validator
from watchfiles import Change, awatch

from ._device import RemoteDevice

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(decouple_config("CONFIG_DIR", default="settings")).resolve()
QOS = 2

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

    def to_hardware_event(self) -> hw_events.HardwareInputMessage:
        if self.gesture == "key_down":
            return hw_events.KeyDownMessage(key_id=self.slot)
        if self.gesture == "key_up":
            return hw_events.KeyUpMessage(key_id=self.slot)
        if self.gesture == "encoder_rotate":
            direction = self.direction
            if direction not in {"clockwise", "counterclockwise"}:
                raise ValueError(
                    f"encoder_rotate mapping for {self.slot!r} requires direction"
                )
            return hw_events.DialRotateMessage(
                dial_id=self.slot,
                direction=direction,
            )
        if self.gesture == "touch_tap":
            return hw_events.TouchTapMessage(touch_id=self.slot)
        if self.gesture == "touch_swipe":
            direction = self.direction
            if direction not in {"left", "right"}:
                raise ValueError(
                    f"touch_swipe mapping for {self.slot!r} requires direction"
                )
            return hw_events.TouchSwipeMessage(
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


def _parse_coordinates(slot_id: str) -> hw_events.HardwareCoordinates:
    parts = slot_id.split(",")
    if len(parts) == 2 and all(part.strip("-").isdigit() for part in parts):
        return hw_events.HardwareCoordinates(column=int(parts[0]), row=int(parts[1]))
    return hw_events.HardwareCoordinates(column=0, row=0)


def _slot_type_for_gestures(gestures: set[RemoteGesture]) -> str:
    if "encoder_rotate" in gestures:
        return "encoder"
    if any(gesture.startswith("touch_") for gesture in gestures):
        return "touch_strip"
    return "button"


def build_slots(mappings: list[RemoteEventMapping]) -> list[hw_events.HardwareSlot]:
    by_slot: dict[str, set[RemoteGesture]] = defaultdict(set)
    for mapping in mappings:
        by_slot[mapping.slot].add(mapping.gesture)
    return [
        hw_events.HardwareSlot(
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
    event_bus: EventBus,
    manager_id: str,
) -> None:
    async for event in device.subscribe():
        await event_bus.send(
            hw_events.hardware_input_message(
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
    event_bus: EventBus,
    manager_id: str,
) -> None:
    async with event_bus.subscribe() as stream:
        async for envelope in stream:
            if hw_events.hardware_manager_id_from_message(envelope) != manager_id:
                continue
            if hw_events.subject_device_id(envelope.subject) != device.id:
                continue
            message = hw_events.hardware_body_from_message(envelope)
            if not isinstance(message, hw_events.HARDWARE_COMMAND_MESSAGE_TYPES):
                continue
            if isinstance(message, hw_events.SetImageMessage):
                await device.set_image(message.slot_id, message.image)
            elif isinstance(message, hw_events.ClearSlotMessage):
                await device.clear_slot(message.slot_id)
            elif isinstance(message, hw_events.SleepScreenMessage):
                await device.sleep_screen()
            elif isinstance(message, hw_events.WakeScreenMessage):
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
                            await device.emit(mapping.to_hardware_event())
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
    event_bus: EventBus,
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

    await event_bus.send(
        hw_events.hardware_input_message(
            manager_id=manager_id,
            device_id=runtime.id,
            body=hw_events.DeviceConnectedMessage(
                device=hw_events.HardwareDevice(
                    id=device.id,
                    hid=device.hid,
                    slots=list(device.slots),
                    name=device.name,
                ),
            ),
        )
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
                event_bus,
                manager_id,
            )
            tg.start_soon(
                _run_until_complete,
                tg.cancel_scope,
                _apply_device_commands,
                device,
                event_bus,
                manager_id,
            )
            tg.start_soon(_run_until_complete, tg.cancel_scope, run_mqtt_loop)
    finally:
        await device.close()
        await event_bus.send(
            hw_events.hardware_input_message(
                manager_id=manager_id,
                device_id=runtime.id,
                body=hw_events.DeviceDisconnectedMessage(),
            )
        )


class RemoteDeviceFactoryComponent(BaseComponent):
    def __init__(
        self,
        bus: EventBus,
        *,
        manager_id: str,
        config_dir: Path = CONFIG_DIR,
        default_mqtt: MqttBrokerDefaults | None = None,
    ):
        super().__init__(name="remote_device_factory")
        self._event_bus = bus
        self._manager_id = manager_id
        self._config_dir = config_dir
        self._default_mqtt = default_mqtt or load_mqtt_broker_defaults()
        self._cancel_scope = None
        self._task_group: anyio.abc.TaskGroup | None = None
        self._stop_event: anyio.Event | None = None
        self._running_devices: dict[str, RunningRemoteDevice] = {}

    async def start(self, ctx: RunContext) -> None:
        self._cancel_scope = ctx.tg.cancel_scope
        self._task_group = ctx.tg
        self._stop_event = anyio.Event()
        await self._reconcile_devices()
        ctx.tg.start_soon(self._watch_loop)
        await anyio.sleep_forever()

    async def stop(self) -> None:
        if self._stop_event is not None:
            self._stop_event.set()
        if self._cancel_scope is not None:
            self._cancel_scope.cancel()

    async def _run_device(self, runtime: RemoteDeviceRuntime) -> None:
        stopped = anyio.Event()
        with anyio.CancelScope() as scope:
            self._running_devices[runtime.id] = RunningRemoteDevice(
                runtime=runtime,
                cancel_scope=scope,
                stopped=stopped,
            )
            try:
                await device_loop(runtime, self._event_bus, self._manager_id)
            finally:
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

        for device_id in running_ids - desired_ids:
            await self._stop_device(device_id)

        for device_id, runtime in desired.items():
            current = self._running_devices.get(device_id)
            if current is None:
                await self._start_device(runtime)
                continue
            if current.runtime != runtime:
                await self._stop_device(device_id)
                await self._start_device(runtime)

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


def driver_factory(
    event_bus: EventBus,
    config: Mapping[str, Any] | None = None,
):
    driver_config = load_driver_config(config)
    return RemoteDeviceFactoryComponent(
        event_bus,
        manager_id=driver_config.manager_id,
        config_dir=driver_config.config_path or CONFIG_DIR,
        default_mqtt=load_mqtt_broker_defaults(config),
    )


def component_factory(context: ComponentContext):
    return driver_factory(
        context.require_lane("hardware_events"),
        config=context.raw_config,
    )


component = ComponentDefinition(
    manifest=ComponentManifest(
        component_id="deckr.drivers.mqtt",
        config_prefix="deckr.drivers.mqtt",
        consumes=("hardware_events",),
        publishes=("hardware_events",),
    ),
    factory=component_factory,
)
