from __future__ import annotations

import logging
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from pathlib import Path
from time import monotonic
from typing import Any

import aiomqtt
import anyio
import deckr.hardware.messages as hw_messages
from deckr.beacon import (
    BEACON_ADVERTISEMENT_STORE_POLICY,
    DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME,
    BeaconDiscovery,
    BeaconService,
)
from deckr.components import (
    BaseComponent,
    ComponentContext,
    ComponentDefinition,
    ComponentManifest,
    RunContext,
)
from deckr.concord import (
    CONCORD_CONTRACT_STORE_POLICY,
    CONCORD_TOKEN_STORE_POLICY,
    DEFAULT_CONCORD_CONTRACT_STORE_NAME,
    DEFAULT_CONCORD_TOKEN_STORE_NAME,
    ConcordCoordinator,
    ConcordService,
)
from deckr.contracts.messages import hardware_manager_address
from deckr.hardware.descriptors import DeviceConnection, DeviceDescriptor
from deckr.hardware.runtime import HardwareManagerRuntime
from deckr.lanes import Lane, RegisteredEndpointLane
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
        beacon: BeaconService,
        concord: ConcordService,
        *,
        manager_id: str,
        base_topic: str = DEFAULT_BASE_TOPIC,
        dedupe_ms: int = DEFAULT_DEDUPE_MS,
        broker: DriverBrokerConfig | None = None,
        labels: Mapping[str, str] | None = None,
    ):
        super().__init__(name="zigbee2mqtt_hardware_manager")
        self._hardware_lane = hardware_lane
        self._beacon = beacon
        self._concord = concord
        self._manager_id = manager_id
        self._base_topic = base_topic.strip().strip("/") or DEFAULT_BASE_TOPIC
        self._dedupe_ms = max(dedupe_ms, 0)
        self._broker = broker or DriverBrokerConfig()
        self._labels = dict(labels or {})
        self._cancel_scope: anyio.CancelScope | None = None
        self._endpoint_cm: (
            AbstractAsyncContextManager[RegisteredEndpointLane] | None
        ) = None
        self._endpoint: RegisteredEndpointLane | None = None
        self._runtime: HardwareManagerRuntime | None = None
        self._stop_event: anyio.Event | None = None
        self._runtimes: dict[str, Zigbee2MqttDeviceRuntime] = {}
        self._runtime_by_topic: dict[str, Zigbee2MqttDeviceRuntime] = {}

    async def start(self, ctx: RunContext) -> None:
        try:
            self._endpoint_cm = self._hardware_lane.register_endpoint(
                hardware_manager_address(self._manager_id),
                metadata={"runtime": "deckr-driver-mqtt-python"},
                task_group=ctx.tg,
            )
            self._endpoint = await self._endpoint_cm.__aenter__()
            self._cancel_scope = ctx.tg.cancel_scope
            self._stop_event = anyio.Event()
            self._runtime = HardwareManagerRuntime(
                endpoint=self._endpoint,
                beacon=self._beacon,
                concord=self._concord,
                manager_id=self._manager_id,
                labels=self._labels,
            )
            await self._runtime.start(ctx.tg)
            ctx.tg.start_soon(self._mqtt_discovery_loop)
        except BaseException:
            with anyio.CancelScope(shield=True):
                await self._stop_runtime()
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
            await self._stop_runtime()
            await self._close_endpoint()

    async def _stop_runtime(self) -> None:
        runtime = self._runtime
        self._runtime = None
        if runtime is not None:
            await runtime.stop()

    async def _close_endpoint(self) -> None:
        endpoint_cm = self._endpoint_cm
        self._endpoint_cm = None
        self._endpoint = None
        if endpoint_cm is not None:
            await endpoint_cm.__aexit__(None, None, None)

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
        if self._runtime is None or self._endpoint is None:
            return
        await self._runtime.handle_hardware_message(
            hw_messages.control_input_message(
                manager_id=self._manager_id,
                sender_session_id=self._endpoint.session_id,
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

        previous_devices = {
            device_id: runtime.descriptor for device_id, runtime in self._runtimes.items()
        }
        next_devices = {
            device_id: runtime.descriptor for device_id, runtime in desired.items()
        }
        self._runtimes = desired
        self._runtime_by_topic = {runtime.topic: runtime for runtime in desired.values()}
        if next_devices == previous_devices:
            return
        if self._runtime is not None:
            await self._runtime.replace_devices(
                next_devices,
                removed_reason="removed",
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


def driver_factory(
    hardware_lane: Lane,
    beacon: BeaconService,
    concord: ConcordService,
    *,
    manager_id: str,
    config: Mapping[str, Any] | None = None,
    config_base_dir: Path | None = None,
):
    driver_config = load_driver_config(config, base_dir=config_base_dir)
    return Zigbee2MqttHardwareManager(
        hardware_lane,
        beacon,
        concord,
        manager_id=manager_id,
        base_topic=driver_config.base_topic,
        dedupe_ms=driver_config.dedupe_ms,
        broker=driver_config.broker,
        labels=driver_config.labels,
    )


def component_factory(context: ComponentContext):
    return driver_factory(
        context.require_lane("hardware_messages"),
        BeaconService(BeaconDiscovery(
            context.state(
                DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME,
                policy=BEACON_ADVERTISEMENT_STORE_POLICY,
            )
        )),
        ConcordService(ConcordCoordinator(
            context.state(
                DEFAULT_CONCORD_CONTRACT_STORE_NAME,
                policy=CONCORD_CONTRACT_STORE_POLICY,
            ),
            context.state(
                DEFAULT_CONCORD_TOKEN_STORE_NAME,
                policy=CONCORD_TOKEN_STORE_POLICY,
            ),
        )),
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
