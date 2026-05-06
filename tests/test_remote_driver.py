import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import anyio
import pytest
from deckr.contracts.lanes import CORE_LANE_CONTRACTS, LaneContractRegistry
from deckr.contracts.messages import (
    EndpointAddress,
    controller_address,
    hardware_manager_address,
)
from deckr.hardware import messages as hw_messages
from deckr.lanes import RegisteredEndpointLane
from deckr.runtime import Deckr
from deckr.state import (
    DEFAULT_DISCOVERY_STATE_STORE_NAME,
    DEFAULT_LEASE_STATE_STORE_NAME,
    DeviceClaim,
    EndpointPresence,
    HardwareInventory,
    StateUnavailable,
    encode_key_token,
    hardware_inventory_key,
    presence_endpoint_key,
)
from memory_lane_substrate import MemoryLaneSubstrate

from deckr.drivers.mqtt._factory import (
    Deduper,
    DriverBrokerConfig,
    Zigbee2MqttHardwareManager,
    _runtime_from_zigbee2mqtt_device,
    driver_factory,
)
from deckr.drivers.mqtt._zigbee2mqtt import (
    build_controls,
    extract_action_values,
    infer_controls,
    parse_bridge_devices,
)

MANAGER_SESSION = "manager-session"
CONTROLLER_SESSION = "controller-session"
PAULMANN_ID = "z2m.0xffffaa6712730330"
PAULMANN_FINGERPRINT = "zigbee2mqtt:0xffffaa6712730330"
PAULMANN_TOPIC = "zigbee2mqtt/remote/0x0330"

PAULMANN_ACTIONS = [
    "on",
    "off",
    "brightness_move_up",
    "brightness_move_down",
    "brightness_stop",
    "brightness_step_up",
    "brightness_step_down",
    "color_temperature_move_up",
    "color_temperature_move_down",
    "color_temperature_move_stop",
    "color_temperature_step_up",
    "color_temperature_step_down",
    "store",
    "recall",
]

HUE_ACTIONS = [
    "on_press",
    "on_press_release",
    "on_hold",
    "on_hold_release",
    "up_press",
    "up_press_release",
    "up_hold",
    "up_hold_release",
    "down_press",
    "down_press_release",
    "down_hold",
    "down_hold_release",
    "off_press",
    "off_press_release",
    "off_hold",
    "off_hold_release",
]

STYRBAR_ACTIONS = [
    "on",
    "off",
    "brightness_move_up",
    "brightness_move_down",
    "brightness_stop",
    "arrow_left_click",
    "arrow_left_hold",
    "arrow_left_release",
    "arrow_right_click",
    "arrow_right_hold",
    "arrow_right_release",
]


class EndpointHarness:
    def __init__(
        self,
        deckr: Deckr,
        endpoint: EndpointAddress,
        *,
        session_id: str,
    ) -> None:
        self._state = deckr.state()
        self._registered = RegisteredEndpointLane(
            lane=deckr.lane("hardware_messages"),
            endpoint=endpoint,
            session_id=session_id,
            state=self._state,
            metadata={"runtime": "test"},
        )

    @property
    def lane(self):
        return self._registered.lane

    @property
    def endpoint(self) -> EndpointAddress:
        return self._registered.endpoint

    @property
    def session_id(self) -> str:
        return self._registered.session_id

    async def _ensure_presence(self) -> None:
        await self._state.put(
            presence_endpoint_key(lane=self.lane.name, endpoint=self.endpoint),
            EndpointPresence(
                endpoint=self.endpoint,
                lane=self.lane.name,
                sessionId=self.session_id,
                timestamp=datetime.now(UTC),
                ttlSeconds=30,
            ),
            ttl=30,
        )

    async def publish(self, message):
        await self._ensure_presence()
        return await self._registered.publish(message)

    @asynccontextmanager
    async def subscribe(self) -> AsyncIterator:
        await self._ensure_presence()
        async with self._registered.subscribe() as stream:
            yield stream


def _endpoint(
    deckr: Deckr,
    endpoint: EndpointAddress,
    *,
    session_id: str = CONTROLLER_SESSION,
) -> EndpointHarness:
    return EndpointHarness(deckr, endpoint, session_id=session_id)


def _deckr() -> Deckr:
    lane_contracts = LaneContractRegistry(CORE_LANE_CONTRACTS.values())
    return Deckr(
        lane_contracts=lane_contracts,
        substrate=MemoryLaneSubstrate(lane_contracts=lane_contracts),
    )


def _factory(deckr: Deckr) -> Zigbee2MqttHardwareManager:
    manager = Zigbee2MqttHardwareManager(
        deckr.lane("hardware_messages"),
        deckr.state(DEFAULT_LEASE_STATE_STORE_NAME),
        deckr.state(DEFAULT_DISCOVERY_STATE_STORE_NAME),
        manager_id="mqtt-main",
        base_topic="zigbee2mqtt",
        dedupe_ms=250,
        broker=DriverBrokerConfig(
            hostname="mqtt-default.local",
            port=1883,
            username=None,
            password=None,
        ),
        labels={"mqtt-host": "mqtt-default.local"},
    )
    manager._endpoint = _endpoint(
        deckr,
        hardware_manager_address("mqtt-main"),
        session_id=MANAGER_SESSION,
    )
    manager._session_id = manager._endpoint.session_id
    return manager


def _claim(controller_id: str = "main", session_id: str = "controller-session"):
    return DeviceClaim(
        claimedByEndpoint=controller_address(controller_id),
        claimedBySessionId=session_id,
        timestamp=datetime.now(UTC),
        ttlSeconds=30,
    )


def _claim_key(device_id: str = PAULMANN_ID) -> str:
    return (
        f"claim.device.{encode_key_token('mqtt-main')}."
        f"{encode_key_token(device_id)}"
    )


async def _put_controller_presence(
    deckr: Deckr,
    *,
    controller_id: str = "main",
    session_id: str = "controller-session",
) -> None:
    endpoint = controller_address(controller_id)
    await deckr.state(DEFAULT_LEASE_STATE_STORE_NAME).put(
        presence_endpoint_key(lane="hardware_messages", endpoint=endpoint),
        EndpointPresence(
            endpoint=endpoint,
            lane="hardware_messages",
            sessionId=session_id,
            timestamp=datetime.now(UTC),
            ttlSeconds=30,
            metadata={},
        ),
    )


def _bridge_payload(*devices: dict) -> str:
    return json.dumps(list(devices))


def _z2m_device(
    *,
    friendly_name: str = "remote/0x0330",
    ieee_address: str = "0xffffaa6712730330",
    vendor: str = "Paulmann",
    model: str = "501.41",
    description: str = "Remote control Smart Home Zigbee 3.0 White",
    actions: list[str] | None = None,
    supported: bool = True,
    disabled: bool = False,
) -> dict:
    return {
        "ieee_address": ieee_address,
        "friendly_name": friendly_name,
        "type": "EndDevice",
        "supported": supported,
        "disabled": disabled,
        "power_source": "Battery",
        "model_id": model.replace(".", ""),
        "interview_state": "SUCCESSFUL",
        "definition": {
            "vendor": vendor,
            "model": model,
            "description": description,
            "exposes": [
                {"type": "numeric", "property": "battery"},
                {
                    "type": "enum",
                    "property": "action",
                    "values": actions or PAULMANN_ACTIONS,
                },
                {"type": "numeric", "property": "linkquality"},
            ],
        },
    }


def _paulmann_device():
    return parse_bridge_devices(_bridge_payload(_z2m_device()))[0]


def _hue_device():
    return parse_bridge_devices(
        _bridge_payload(
            _z2m_device(
                friendly_name="switch/huedimmer/0x8e62",
                ieee_address="0x0017880108758e62",
                vendor="Philips",
                model="324131092621",
                description="Hue dimmer switch",
                actions=HUE_ACTIONS,
            )
        )
    )[0]


def _styrbar_device():
    return parse_bridge_devices(
        _bridge_payload(
            _z2m_device(
                friendly_name="switch/styrbar/0x94e9",
                ieee_address="0x94b216fffe6794e9",
                vendor="IKEA",
                model="E2001/E2002",
                description="STYRBAR remote control",
                actions=STYRBAR_ACTIONS,
            )
        )
    )[0]


def test_extract_action_values_supports_plain_and_json_payloads():
    assert extract_action_values("off") == ("off",)
    assert extract_action_values(b'{"action":"brightness_step_up","battery":86}') == (
        "brightness_step_up",
    )
    assert extract_action_values('"off"') == ("off",)
    assert extract_action_values('{"action":""}') == ()
    assert extract_action_values('{"action":""}', include_empty=True) == ("",)
    assert extract_action_values('{"battery":86}') == ()
    assert extract_action_values('{"action":86}') == ()
    assert extract_action_values('["off"]') == ()
    assert extract_action_values("86") == ()


def test_infer_paulmann_controls_as_core_buttons():
    controls = {control.control_id: control for control in infer_controls(PAULMANN_ACTIONS)}

    assert controls["on"].capability_ids == ("button.press",)
    assert controls["off"].capability_ids == ("button.press",)
    assert controls["brightness_up"].capability_ids == (
        "button.momentary",
        "button.press",
    )
    assert controls["brightness_down"].capability_ids == (
        "button.momentary",
        "button.press",
    )
    assert controls["color_temperature_up"].capability_ids == (
        "button.momentary",
        "button.press",
    )
    assert controls["store"].capability_ids == ("button.press",)
    assert controls["store_1"].capability_ids == ("button.press",)
    assert controls["store_2"].capability_ids == ("button.press",)
    assert "brightness_stop" not in controls
    assert controls["recall"].capability_ids == ("button.press",)
    assert controls["recall_1"].capability_ids == ("button.press",)
    assert controls["recall_2"].capability_ids == ("button.press",)


def test_infer_hue_dimmer_press_release_and_hold_as_momentary_buttons():
    controls = {control.control_id: control for control in infer_controls(HUE_ACTIONS)}

    assert set(controls) == {"on", "up", "down", "off"}
    assert controls["on"].capability_ids == ("button.momentary",)
    assert {action.event_type for action in controls["on"].actions} == {"down", "up"}


def test_infer_styrbar_mixed_simple_move_and_click_hold_release_controls():
    controls = {control.control_id: control for control in infer_controls(STYRBAR_ACTIONS)}

    assert controls["on"].capability_ids == ("button.press",)
    assert controls["brightness_up"].capability_ids == ("button.momentary",)
    assert controls["arrow_left"].capability_ids == (
        "button.press",
        "button.momentary",
    )


def test_build_controls_never_emits_fake_encoder_capabilities():
    controls = build_controls(PAULMANN_ACTIONS)

    assert {control.kind for control in controls} == {"button"}
    assert {
        capability.capability_id
        for control in controls
        for capability in control.input_capabilities
    } == {"button.press", "button.momentary"}


def test_runtime_maps_json_actions_to_core_button_events():
    runtime = _runtime_from_zigbee2mqtt_device(_paulmann_device(), dedupe_ms=0)

    assert runtime is not None
    move = runtime.events_for_payload('{"action":"brightness_move_up","battery":86}')
    stop = runtime.events_for_payload('{"action":"brightness_stop"}')
    orphan_stop = runtime.events_for_payload('{"action":"brightness_stop"}')
    step = runtime.events_for_payload('{"action":"color_temperature_step_down"}')
    scene_1 = runtime.events_for_payload('{"action":"recall_1"}')
    scene_2 = runtime.events_for_payload('{"action":"recall_2"}')

    assert move[0].control_id == "brightness_up"
    assert move[0].capability_id == "button.momentary"
    assert move[0].event_type == "down"
    assert move[0].value == {"eventType": "down"}
    assert stop[0].control_id == "brightness_up"
    assert stop[0].event_type == "up"
    assert orphan_stop == ()
    assert step[0].control_id == "color_temperature_down"
    assert step[0].capability_id == "button.press"
    assert step[0].event_type == "press"
    assert scene_1[0].control_id == "recall_1"
    assert scene_1[0].capability_id == "button.press"
    assert scene_1[0].event_type == "press"
    assert scene_2[0].control_id == "recall_2"
    assert scene_2[0].capability_id == "button.press"
    assert scene_2[0].event_type == "press"


def test_runtime_keeps_active_stop_state_per_device_axis():
    paulmann = _runtime_from_zigbee2mqtt_device(_paulmann_device(), dedupe_ms=0)
    styrbar = _runtime_from_zigbee2mqtt_device(_styrbar_device(), dedupe_ms=0)

    assert paulmann is not None
    assert styrbar is not None
    paulmann.events_for_payload('{"action":"brightness_move_down"}')

    assert styrbar.events_for_payload('{"action":"brightness_stop"}') == ()
    assert paulmann.events_for_payload('{"action":"brightness_stop"}')[0].control_id == (
        "brightness_down"
    )


def test_runtime_maps_hue_and_styrbar_gestures():
    hue = _runtime_from_zigbee2mqtt_device(_hue_device(), dedupe_ms=0)
    styrbar = _runtime_from_zigbee2mqtt_device(_styrbar_device(), dedupe_ms=0)

    assert hue is not None
    assert styrbar is not None
    assert hue.events_for_payload('{"action":"up_press"}')[0].event_type == "down"
    assert hue.events_for_payload('{"action":"up_press_release"}')[0].event_type == "up"
    assert styrbar.events_for_payload('{"action":"arrow_left_click"}')[0].capability_id == (
        "button.press"
    )
    assert styrbar.events_for_payload('{"action":"arrow_left_hold"}')[0].event_type == (
        "down"
    )
    assert styrbar.events_for_payload('{"action":"arrow_left_release"}')[0].event_type == (
        "up"
    )


def test_deduper_suppresses_duplicate_actions_within_window():
    deduper = Deduper(dedupe_ms=500)

    assert deduper.should_emit("off") is True
    assert deduper.should_emit("off") is False


def test_runtime_respects_action_dedupe_window():
    runtime = _runtime_from_zigbee2mqtt_device(_paulmann_device(), dedupe_ms=500)

    assert runtime is not None
    assert runtime.events_for_payload('{"action":"on"}')
    assert runtime.events_for_payload('{"action":"on"}') == ()


def test_orphan_stop_does_not_poison_next_real_stop():
    runtime = _runtime_from_zigbee2mqtt_device(_paulmann_device(), dedupe_ms=500)

    assert runtime is not None
    assert runtime.events_for_payload('{"action":"brightness_stop"}') == ()
    assert runtime.events_for_payload('{"action":"brightness_move_up"}')[0].event_type == (
        "down"
    )
    stop = runtime.events_for_payload('{"action":"brightness_stop"}')

    assert stop[0].control_id == "brightness_up"
    assert stop[0].event_type == "up"


@pytest.mark.asyncio
async def test_driver_factory_reads_discovery_config_and_labels():
    async with _deckr() as deckr:
        component = driver_factory(
            deckr.lane("hardware_messages"),
            deckr.state(DEFAULT_LEASE_STATE_STORE_NAME),
            deckr.state(DEFAULT_DISCOVERY_STATE_STORE_NAME),
            manager_id="mqtt-main",
            config={
                "base_topic": "zigbee2mqtt/",
                "dedupe_ms": 300,
                "broker": {"hostname": "openhabian", "port": 1884},
                "labels": {"mqtt-host": "openhabian"},
            },
        )

        assert component._base_topic == "zigbee2mqtt"
        assert component._dedupe_ms == 300
        assert component._broker.hostname == "openhabian"
        assert component._broker.port == 1884
        assert component._labels == {"mqtt-host": "openhabian"}


@pytest.mark.asyncio
async def test_driver_factory_rejects_old_config_file_keys(tmp_path):
    async with _deckr() as deckr:
        for config in (
            {"config_path": str(tmp_path)},
            {"devices_path": str(tmp_path)},
            {"templates_path": str(tmp_path)},
        ):
            with pytest.raises(ValueError):
                driver_factory(
                    deckr.lane("hardware_messages"),
                    deckr.state(DEFAULT_LEASE_STATE_STORE_NAME),
                    deckr.state(DEFAULT_DISCOVERY_STATE_STORE_NAME),
                    manager_id="mqtt-main",
                    config=config,
                )


@pytest.mark.asyncio
async def test_reconcile_discovered_devices_publishes_aggregate_inventory():
    bridge = parse_bridge_devices(
        _bridge_payload(
            _z2m_device(),
            _z2m_device(
                friendly_name="disabled/remote",
                ieee_address="0x1111",
                disabled=True,
            ),
            _z2m_device(
                friendly_name="unsupported/remote",
                ieee_address="0x2222",
                supported=False,
            ),
            {
                "friendly_name": "sensor/no-action",
                "definition": {"exposes": [{"property": "battery"}]},
            },
        )
    )

    async with _deckr() as deckr:
        component = _factory(deckr)
        await component._reconcile_discovered_devices(bridge)

        entry = await deckr.state(DEFAULT_DISCOVERY_STATE_STORE_NAME).get(
            hardware_inventory_key("mqtt-main")
        )
        assert entry is not None
        inventory = HardwareInventory.model_validate(entry.value)
        assert inventory.labels == {"mqtt-host": "mqtt-default.local"}
        assert set(inventory.devices) == {PAULMANN_ID}
        device = inventory.devices[PAULMANN_ID]
        assert device.device_ref.fingerprint == PAULMANN_FINGERPRINT
        assert device.descriptor.device_id == PAULMANN_ID
        assert device.descriptor.fingerprint == PAULMANN_FINGERPRINT


@pytest.mark.asyncio
async def test_inventory_state_unavailable_keeps_discovered_device():
    class UnavailableState:
        async def put(self, *args):
            raise StateUnavailable("temporary substrate outage")

    async with _deckr() as deckr:
        component = Zigbee2MqttHardwareManager(
            deckr.lane("hardware_messages"),
            deckr.state(DEFAULT_LEASE_STATE_STORE_NAME),
            UnavailableState(),
            manager_id="mqtt-main",
            broker=DriverBrokerConfig(
                hostname="mqtt-default.local",
                port=1883,
                username=None,
                password=None,
            ),
        )
        component._endpoint = _endpoint(
            deckr,
            hardware_manager_address("mqtt-main"),
            session_id=MANAGER_SESSION,
        )
        component._session_id = component._endpoint.session_id
        await component._reconcile_discovered_devices((_paulmann_device(),))

    assert PAULMANN_ID in component._devices
    assert component._inventory_revision is None


@pytest.mark.asyncio
async def test_claimed_mqtt_input_is_sent_only_to_claiming_controller():
    async with _deckr() as deckr:
        component = _factory(deckr)
        await component._reconcile_discovered_devices((_paulmann_device(),))
        component._claims[PAULMANN_ID] = _claim()
        component._controller_presence_sessions[controller_address("main")] = (
            "controller-session"
        )
        main = _endpoint(deckr, controller_address("main"))
        other = _endpoint(deckr, controller_address("other"), session_id="other-session")

        async with main.subscribe() as main_stream, other.subscribe() as other_stream:
            await component._handle_mqtt_device_payload(
                topic=PAULMANN_TOPIC,
                payload='{"action":"on"}',
            )
            received = await main_stream.receive()
            with anyio.move_on_after(0.05) as scope:
                await other_stream.receive()

    body = hw_messages.hardware_body_from_message(received)
    assert received.recipient.endpoint == controller_address("main")
    assert isinstance(body, hw_messages.ControlInputMessage)
    assert body.control_id == "on"
    assert body.capability_id == "button.press"
    assert body.event_type == "press"
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_broker_snapshot_claim_delete_drops_input():
    async with _deckr() as deckr:
        component = _factory(deckr)
        await component._reconcile_discovered_devices((_paulmann_device(),))
        await _put_controller_presence(deckr)
        claim_key = _claim_key()
        await deckr.state().create(claim_key, _claim())
        await component._reconcile_routing_current_state(reason="test snapshot")
        main = _endpoint(deckr, controller_address("main"))

        async with main.subscribe() as main_stream:
            await deckr.state().delete(claim_key)
            await component._reconcile_routing_current_state(reason="test snapshot")
            await component._handle_mqtt_device_payload(
                topic=PAULMANN_TOPIC,
                payload='{"action":"on"}',
            )
            with anyio.move_on_after(0.05) as scope:
                await main_stream.receive()

    assert scope.cancel_called


@pytest.mark.asyncio
async def test_prefix_observation_omissions_keep_current_routing(monkeypatch):
    async with _deckr() as deckr:
        component = _factory(deckr)
        await component._reconcile_discovered_devices((_paulmann_device(),))
        await _put_controller_presence(deckr)
        await deckr.state().create(_claim_key(), _claim())
        await component._reconcile_routing_current_state(reason="initial snapshot")
        assert component._claim_recipient(PAULMANN_ID) == controller_address("main")

        async def omitted_items(prefix: str = ""):
            del prefix
            return ()

        monkeypatch.setattr(
            deckr.state(DEFAULT_LEASE_STATE_STORE_NAME),
            "items",
            omitted_items,
        )

        await component._reconcile_routing_current_state(reason="omitted snapshot")

        assert component._claim_recipient(PAULMANN_ID) == controller_address("main")
        assert PAULMANN_ID in component._claims


@pytest.mark.asyncio
async def test_controller_presence_restore_makes_current_claim_routable():
    async with _deckr() as deckr:
        component = _factory(deckr)
        await component._reconcile_discovered_devices((_paulmann_device(),))
        await deckr.state().create(_claim_key(), _claim())
        await component._reconcile_routing_current_state(reason="test snapshot")
        assert component._claim_recipient(PAULMANN_ID) is None

        await _put_controller_presence(deckr)
        await component._reconcile_routing_current_state(reason="test snapshot")
        assert component._claim_recipient(PAULMANN_ID) == controller_address("main")


@pytest.mark.asyncio
async def test_invalid_claim_payload_is_not_routable():
    async with _deckr() as deckr:
        component = _factory(deckr)
        await component._reconcile_discovered_devices((_paulmann_device(),))
        await deckr.state().put(
            _claim_key(),
            {
                "claimedByEndpoint": "controller:main",
                "timestamp": datetime.now(UTC).isoformat(),
                "ttlSeconds": 30,
            },
        )
        await _put_controller_presence(deckr)
        await component._reconcile_routing_current_state(reason="test snapshot")

    assert PAULMANN_ID not in component._claims
