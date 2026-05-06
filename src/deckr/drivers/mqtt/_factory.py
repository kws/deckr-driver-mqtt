from __future__ import annotations

import logging
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any

import aiomqtt
import anyio
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
from deckr.hardware.descriptors import (
    DeviceConnection,
    DeviceDescriptor,
    DeviceRef,
)
from deckr.lanes import Lane, RegisteredEndpointLane
from deckr.state import (
    DEFAULT_DISCOVERY_STATE_STORE_NAME,
    DEFAULT_LEASE_STATE_STORE_NAME,
    DeviceClaim,
    EndpointPresence,
    HardwareInventory,
    HardwareInventoryDevice,
    StateConflict,
    StateStore,
    StateUnavailable,
    encode_key_token,
    hardware_inventory_key,
    observe_prefix_current,
    parse_device_claim_key,
    parse_presence_endpoint_key,
    presence_endpoint_key,
)
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ._zigbee2mqtt import (
    DEFAULT_BASE_TOPIC,
    DEFAULT_DEDUPE_MS,
    QOS,
    InferredAction,
    Zigbee2MqttDevice,
    build_controls,
    extract_action_values,
    infer_actions,
    parse_bridge_devices,
    topic_text,
)

logger = logging.getLogger(__name__)

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


class _DriverConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DriverBrokerConfig(_DriverConfigModel):
    hostname: str = ""
    port: int = 1883
    username: str | None = None
    password: str | None = None


class DriverConfig(_DriverConfigModel):
    base_topic: str = DEFAULT_BASE_TOPIC
    dedupe_ms: int = DEFAULT_DEDUPE_MS
    broker: DriverBrokerConfig = Field(default_factory=DriverBrokerConfig)
    labels: dict[str, str] = Field(default_factory=dict)

    @field_validator("base_topic")
    @classmethod
    def _normalize_base_topic(cls, value: str) -> str:
        normalized = value.strip().strip("/")
        if not normalized:
            raise ValueError("base_topic must not be empty")
        return normalized

    @field_validator("dedupe_ms")
    @classmethod
    def _normalize_dedupe_ms(cls, value: int) -> int:
        return max(value, 0)


@dataclass(frozen=True, slots=True)
class ControlInputEvent:
    control_id: str
    capability_id: str
    event_type: str
    value: dict[str, Any]


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


@dataclass(slots=True)
class Zigbee2MqttDeviceRuntime:
    device: Zigbee2MqttDevice
    descriptor: DeviceDescriptor
    mappings_by_action: Mapping[str, InferredAction]
    deduper: Deduper
    active_axes: dict[str, str] = field(default_factory=dict)

    @property
    def id(self) -> str:
        return self.device.device_id

    @property
    def topic(self) -> str:
        return self.device.topic

    @property
    def fingerprint(self) -> str:
        return self.device.fingerprint

    def events_for_payload(self, payload: bytes | str) -> tuple[ControlInputEvent, ...]:
        events: list[ControlInputEvent] = []
        for action in extract_action_values(payload):
            mapping = self.mappings_by_action.get(action)
            if mapping is None:
                logger.debug(
                    "Ignoring unmapped Zigbee2MQTT action %s for %s",
                    action,
                    self.device.friendly_name,
                )
                continue
            if not mapping.stop and not self.deduper.should_emit(action):
                continue
            event = self._event_for_mapping(mapping)
            if event is not None:
                logger.debug(
                    "Mapped Zigbee2MQTT action %s for %s to %s/%s/%s",
                    action,
                    self.device.friendly_name,
                    event.control_id,
                    event.capability_id,
                    event.event_type,
                )
                events.append(event)
        return tuple(events)

    def _event_for_mapping(
        self,
        mapping: InferredAction,
    ) -> ControlInputEvent | None:
        control_id = mapping.control_id
        if mapping.stop:
            if mapping.axis is None:
                return None
            control_id = self.active_axes.pop(mapping.axis, None)
            if control_id is None:
                logger.debug(
                    "Dropping Zigbee2MQTT stop action %s without active direction",
                    mapping.action,
                )
                return None
        elif mapping.axis is not None and mapping.event_type == "down":
            self.active_axes[mapping.axis] = mapping.control_id or ""

        if not control_id:
            return None
        return ControlInputEvent(
            control_id=control_id,
            capability_id=mapping.capability_id,
            event_type=mapping.event_type,
            value={"eventType": mapping.event_type},
        )


def load_driver_config(
    config: Mapping[str, Any] | None = None,
    *,
    base_dir: Path | None = None,
) -> DriverConfig:
    del base_dir
    return DriverConfig.model_validate(dict(config or {}))


class Zigbee2MqttHardwareManager(BaseComponent):
    def __init__(
        self,
        hardware_lane: Lane,
        lease_state: StateStore,
        discovery_state: StateStore,
        *,
        manager_id: str,
        base_topic: str = DEFAULT_BASE_TOPIC,
        dedupe_ms: int = DEFAULT_DEDUPE_MS,
        broker: DriverBrokerConfig | None = None,
        labels: Mapping[str, str] | None = None,
    ):
        super().__init__(name="zigbee2mqtt_hardware_manager")
        self._hardware_lane = hardware_lane
        self._lease_state = lease_state
        self._discovery_state = discovery_state
        self._manager_id = manager_id
        self._base_topic = base_topic.strip().strip("/") or DEFAULT_BASE_TOPIC
        self._dedupe_ms = max(dedupe_ms, 0)
        self._broker = broker or DriverBrokerConfig()
        self._labels = dict(labels or {})
        self._session_id = ""
        self._cancel_scope: anyio.CancelScope | None = None
        self._endpoint_cm: (
            AbstractAsyncContextManager[RegisteredEndpointLane] | None
        ) = None
        self._endpoint: RegisteredEndpointLane | None = None
        self._stop_event: anyio.Event | None = None
        self._runtimes: dict[str, Zigbee2MqttDeviceRuntime] = {}
        self._runtime_by_topic: dict[str, Zigbee2MqttDeviceRuntime] = {}
        self._devices: dict[str, DeviceDescriptor] = {}
        self._claims: dict[str, DeviceClaim] = {}
        self._controller_presence_sessions: dict[EndpointAddress, str] = {}
        self._inventory_revision: int | None = None
        self._inventory_dirty = False
        self._routing_reconcile_lock = anyio.Lock()

    async def start(self, ctx: RunContext) -> None:
        try:
            self._endpoint_cm = self._hardware_lane.register_endpoint(
                hardware_manager_address(self._manager_id),
                metadata={"runtime": "deckr-driver-mqtt-python"},
                task_group=ctx.tg,
            )
            self._endpoint = await self._endpoint_cm.__aenter__()
            self._session_id = self._endpoint.session_id
            self._cancel_scope = ctx.tg.cancel_scope
            self._stop_event = anyio.Event()
            await self._publish_inventory_safely()
            ctx.tg.start_soon(self._command_subscription_loop)
            ctx.tg.start_soon(self._claim_watch_loop)
            ctx.tg.start_soon(self._controller_presence_loop)
            ctx.tg.start_soon(self._routing_reconciliation_loop)
            ctx.tg.start_soon(self._inventory_retry_loop)
            ctx.tg.start_soon(self._mqtt_discovery_loop)
        except BaseException:
            with anyio.CancelScope(shield=True):
                await self._withdraw_inventory()
                await self._close_endpoint()
            raise

    async def stop(self) -> None:
        with anyio.CancelScope(shield=True):
            if self._stop_event is not None:
                self._stop_event.set()
            if self._cancel_scope is not None:
                self._cancel_scope.cancel()
            self._runtimes.clear()
            self._runtime_by_topic.clear()
            self._devices.clear()
            self._claims.clear()
            await self._withdraw_inventory()
            await self._close_endpoint()

    async def _mqtt_discovery_loop(self) -> None:
        if not self._broker.hostname:
            logger.warning("MQTT hardware manager has no broker hostname configured")
            return

        backoff = 1.0
        cancelled_exc = anyio.get_cancelled_exc_class()
        bridge_topic = f"{self._base_topic}/bridge/devices"
        while True:
            subscribed_device_topics: set[str] = set()
            try:
                async with aiomqtt.Client(
                    self._broker.hostname,
                    port=self._broker.port,
                    username=self._broker.username,
                    password=self._broker.password,
                ) as client:
                    await client.subscribe(bridge_topic, qos=QOS)
                    logger.info(
                        "Zigbee2MQTT manager %s subscribed to %s",
                        self._manager_id,
                        bridge_topic,
                    )
                    backoff = 1.0
                    async for message in client.messages:
                        message_topic = topic_text(message.topic)
                        if message_topic == bridge_topic:
                            await self._handle_bridge_devices_payload(
                                message.payload,
                                client=client,
                                subscribed_device_topics=subscribed_device_topics,
                            )
                            continue
                        await self._handle_mqtt_device_payload(
                            topic=message_topic,
                            payload=message.payload,
                        )
            except cancelled_exc:
                raise
            except Exception:
                logger.exception(
                    "Zigbee2MQTT manager %s disconnected; retrying in %.1fs",
                    self._manager_id,
                    backoff,
                )
                await anyio.sleep(backoff)
                backoff = min(backoff * 2.0, 10.0)

    async def _handle_bridge_devices_payload(
        self,
        payload: bytes | str,
        *,
        client: aiomqtt.Client,
        subscribed_device_topics: set[str],
    ) -> None:
        try:
            devices = parse_bridge_devices(payload, base_topic=self._base_topic)
        except Exception:
            logger.exception("Ignoring invalid Zigbee2MQTT bridge/devices payload")
            return
        await self._reconcile_discovered_devices(devices)
        desired_topics = {runtime.topic for runtime in self._runtimes.values()}
        for topic in sorted(desired_topics - subscribed_device_topics):
            await client.subscribe(topic, qos=QOS)
            subscribed_device_topics.add(topic)
        for topic in sorted(subscribed_device_topics - desired_topics):
            try:
                await client.unsubscribe(topic)
            except Exception:
                logger.debug("Could not unsubscribe from removed MQTT topic %s", topic)
            subscribed_device_topics.discard(topic)

    async def _handle_mqtt_device_payload(self, *, topic: str, payload: bytes | str) -> None:
        runtime = self._runtime_by_topic.get(topic)
        if runtime is None:
            return
        for event in runtime.events_for_payload(payload):
            await self._publish_control_input(runtime, event)

    async def _publish_control_input(
        self,
        runtime: Zigbee2MqttDeviceRuntime,
        event: ControlInputEvent,
    ) -> None:
        await self._handle_device_message(
            hw_messages.control_input_message(
                manager_id=self._manager_id,
                sender_session_id=self._session_id,
                device_id=runtime.id,
                fingerprint=runtime.fingerprint,
                control_id=event.control_id,
                capability_id=event.capability_id,
                event_type=event.event_type,
                value=event.value,
            )
        )

    async def _reconcile_discovered_devices(
        self,
        devices: tuple[Zigbee2MqttDevice, ...],
    ) -> None:
        desired: dict[str, Zigbee2MqttDeviceRuntime] = {}
        for device in devices:
            runtime = _runtime_from_zigbee2mqtt_device(device, dedupe_ms=self._dedupe_ms)
            if runtime is None:
                continue
            previous = self._runtimes.get(runtime.id)
            if (
                previous is not None
                and previous.topic == runtime.topic
                and previous.descriptor == runtime.descriptor
                and previous.mappings_by_action == runtime.mappings_by_action
            ):
                desired[runtime.id] = previous
            else:
                desired[runtime.id] = runtime

        previous_devices = self._devices
        next_devices = {
            device_id: runtime.descriptor for device_id, runtime in desired.items()
        }
        self._runtimes = desired
        self._runtime_by_topic = {runtime.topic: runtime for runtime in desired.values()}
        if next_devices == previous_devices:
            return

        removed_devices = set(previous_devices) - set(next_devices)
        self._devices = next_devices
        for device_id in removed_devices:
            self._claims.pop(device_id, None)
        await self._publish_inventory_safely()
        if self._endpoint is None:
            return
        for device_id in sorted(removed_devices):
            await self._endpoint.publish(
                hw_messages.device_unavailable_message(
                    manager_id=self._manager_id,
                    sender_session_id=self._endpoint.session_id,
                    device_id=device_id,
                    reason="removed",
                )
            )
        for device_id, descriptor in sorted(next_devices.items()):
            if device_id not in previous_devices:
                await self._endpoint.publish(
                    hw_messages.device_available_message(
                        manager_id=self._manager_id,
                        sender_session_id=self._endpoint.session_id,
                        descriptor=descriptor,
                    )
                )
            elif previous_devices[device_id] != descriptor:
                await self._endpoint.publish(
                    hw_messages.device_descriptor_changed_message(
                        manager_id=self._manager_id,
                        sender_session_id=self._endpoint.session_id,
                        descriptor=descriptor,
                    )
                )

    async def _close_endpoint(self) -> None:
        endpoint_cm = self._endpoint_cm
        self._endpoint_cm = None
        self._endpoint = None
        if endpoint_cm is not None:
            await endpoint_cm.__aexit__(None, None, None)

    async def _handle_device_message(self, message: DeckrMessage) -> None:
        if self._endpoint is None:
            return
        event = hw_messages.hardware_body_from_message(message)
        ref = hw_messages.hardware_device_ref_from_message(message)
        if ref is None:
            return
        if not isinstance(
            event,
            hw_messages.ControlInputMessage | hw_messages.CapabilityStateChangedMessage,
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
                sender_session_id=self._endpoint.session_id,
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
        entry = await self._discovery_state.put(
            hardware_inventory_key(self._manager_id),
            HardwareInventory(
                managerId=self._manager_id,
                managerEndpoint=self._endpoint.endpoint,
                sessionId=self._session_id,
                timestamp=datetime.now(UTC),
                labels=self._labels,
                devices={
                    device_id: HardwareInventoryDevice(
                        deviceRef=DeviceRef(
                            managerId=self._manager_id,
                            deviceId=device_id,
                            fingerprint=device.fingerprint,
                        ),
                        descriptor=device,
                    )
                    for device_id, device in sorted(self._devices.items())
                },
            ),
        )
        self._inventory_revision = entry.revision

    async def _publish_inventory_safely(self) -> None:
        try:
            await self._publish_inventory()
            self._inventory_dirty = False
        except StateUnavailable:
            self._inventory_dirty = True
            logger.warning(
                "MQTT inventory current state is unavailable; dirty inventory "
                "publish will retry",
                exc_info=True,
            )

    async def _inventory_retry_loop(self) -> None:
        while True:
            await anyio.sleep(5)
            if self._inventory_dirty:
                await self._publish_inventory_safely()

    async def _withdraw_inventory(self) -> None:
        revision = self._inventory_revision
        if revision is None:
            return
        with anyio.CancelScope(shield=True):
            try:
                await self._discovery_state.delete(
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
                async with self._lease_state.watch(prefix) as stream:
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
                async with self._lease_state.watch(
                    _CONTROLLER_PRESENCE_PREFIX
                ) as stream:
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
        claim_observation = await observe_prefix_current(
            self._lease_state,
            claim_prefix,
            known_keys=(
                f"claim.device.{encode_key_token(self._manager_id)}."
                f"{encode_key_token(device_id)}"
                for device_id in self._claims
            ),
        )
        presence_observation = await observe_prefix_current(
            self._lease_state,
            _CONTROLLER_PRESENCE_PREFIX,
            known_keys=(
                presence_endpoint_key(lane="hardware_messages", endpoint=endpoint)
                for endpoint in self._controller_presence_sessions
            ),
        )

        next_claims = dict(self._claims)
        next_controller_sessions = dict(self._controller_presence_sessions)

        for key in claim_observation.confirmed_missing:
            parsed = parse_device_claim_key(key)
            if parsed is None:
                continue
            manager_id, device_id = parsed
            if manager_id == self._manager_id:
                next_claims.pop(device_id, None)

        for entry in claim_observation.entries:
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
                next_claims.pop(device_id, None)

        for key in presence_observation.confirmed_missing:
            parsed = parse_presence_endpoint_key(key)
            if parsed is None:
                continue
            lane, endpoint = parsed
            if lane == "hardware_messages" and endpoint.family == "controller":
                next_controller_sessions.pop(endpoint, None)

        for entry in presence_observation.entries:
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
        self._claims = next_claims
        self._controller_presence_sessions = next_controller_sessions

    def _claim_recipient(self, device_id: str) -> EndpointAddress | None:
        claim = self._claims.get(device_id)
        if claim is None:
            return None
        return _claim_recipient(claim, self._controller_presence_sessions)

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
        if not isinstance(
            message,
            hw_messages.ControlCommandMessage | hw_messages.CapabilityStateRequestMessage,
        ):
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
        logger.debug(
            "Dropping command for input-only Zigbee2MQTT device %s/%s",
            ref.manager_id,
            ref.device_id,
        )


def _runtime_from_zigbee2mqtt_device(
    device: Zigbee2MqttDevice,
    *,
    dedupe_ms: int,
) -> Zigbee2MqttDeviceRuntime | None:
    if not device.is_action_device:
        return None
    mappings = infer_actions(device.actions)
    if not any(mapping.control_id is not None for mapping in mappings):
        logger.debug(
            "Skipping Zigbee2MQTT device %s because no actions could be inferred",
            device.friendly_name,
        )
        return None
    descriptor = _hardware_device_from_zigbee2mqtt_device(device)
    return Zigbee2MqttDeviceRuntime(
        device=device,
        descriptor=descriptor,
        mappings_by_action={mapping.action: mapping for mapping in mappings},
        deduper=Deduper(dedupe_ms),
    )


def _hardware_device_from_zigbee2mqtt_device(
    device: Zigbee2MqttDevice,
) -> DeviceDescriptor:
    return DeviceDescriptor(
        deviceId=device.device_id,
        fingerprint=device.fingerprint,
        displayName=device.display_name,
        manufacturer=device.vendor or "Zigbee2MQTT",
        model=device.model or device.model_id or "Zigbee2MQTT action device",
        modelId=device.model_id,
        serialNumber=device.ieee_address,
        connections=(
            DeviceConnection(
                connectionId=f"mqtt-{device.device_id}",
                type="mqtt",
                status="available",
                transport="mqtt",
                facts={
                    key: value
                    for key, value in {
                        "topic": device.topic,
                        "friendly_name": device.friendly_name,
                        "ieee_address": device.ieee_address,
                        "power_source": device.power_source,
                        "interview_state": device.interview_state,
                    }.items()
                    if value is not None
                },
            ),
        ),
        controls=build_controls(device.actions),
    )


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
    lease_state: StateStore,
    discovery_state: StateStore,
    *,
    manager_id: str,
    config: Mapping[str, Any] | None = None,
    config_base_dir: Path | None = None,
):
    driver_config = load_driver_config(config, base_dir=config_base_dir)
    return Zigbee2MqttHardwareManager(
        hardware_lane,
        lease_state,
        discovery_state,
        manager_id=manager_id,
        base_topic=driver_config.base_topic,
        dedupe_ms=driver_config.dedupe_ms,
        broker=driver_config.broker,
        labels=driver_config.labels,
    )


def component_factory(context: ComponentContext):
    return driver_factory(
        context.require_lane("hardware_messages"),
        context.state(DEFAULT_LEASE_STATE_STORE_NAME),
        context.state(DEFAULT_DISCOVERY_STATE_STORE_NAME),
        manager_id=context.require_endpoint_id("hardware_manager"),
        config=context.config,
        config_base_dir=context.base_dir,
    )


component = ComponentDefinition(
    manifest=ComponentManifest(
        component_id="dev.deckr.hardware.mqtt",
        consumes=("hardware_messages",),
        publishes=("hardware_messages",),
        endpoint_slots=("hardware_manager",),
        role="hardware_manager",
    ),
    factory=component_factory,
)
