import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import anyio
import pytest
from deckr.contracts.lanes import CORE_LANE_CONTRACTS, LaneContractRegistry
from deckr.contracts.messages import (
    EndpointAddress,
    controller_address,
    hardware_manager_address,
)
from deckr.hardware import messages as hw_messages
from deckr.hardware.descriptors import CapabilityRef, DeviceRef
from deckr.lanes import RegisteredEndpointLane
from deckr.runtime import Deckr
from deckr.state import (
    DEFAULT_DISCOVERY_STATE_STORE_NAME,
    DEFAULT_LEASE_STATE_STORE_NAME,
    DeviceClaim,
    EndpointPresence,
    HardwareInventory,
    StateUnavailable,
    hardware_inventory_key,
    presence_endpoint_key,
)
from memory_lane_substrate import MemoryLaneSubstrate

from deckr.drivers.mqtt._factory import (
    Deduper,
    MqttBrokerDefaults,
    RemoteDeviceFactoryComponent,
    RemoteEventMapping,
    RuntimeRemoteMapping,
    _apply_device_commands,
    _extract_action_values,
    build_controls,
    load_remote_devices,
)

MANAGER_SESSION = "manager-session"
CONTROLLER_SESSION = "controller-session"


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


def _factory(deckr: Deckr, config_dir: Path) -> RemoteDeviceFactoryComponent:
    manager = RemoteDeviceFactoryComponent(
        deckr.lane("hardware_messages"),
        deckr.state(DEFAULT_LEASE_STATE_STORE_NAME),
        deckr.state(DEFAULT_DISCOVERY_STATE_STORE_NAME),
        manager_id="mqtt-main",
        config_dir=config_dir,
        default_mqtt=MqttBrokerDefaults(
            hostname="mqtt-default.local",
            port=1883,
            username=None,
            password=None,
        ),
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


def _write_remote_config(
    path: Path,
    *,
    device_id: str = "remote-0x0330",
    control_id: str = "0,0",
    topic: str = "zigbee2mqtt/remote/0x0330/action",
) -> None:
    path.write_text(
        f"""
id: {device_id}
name: Zigbee remote
remote:
  mqtt:
    hostname: mqtt-a.local
    topic: {topic}
  events:
    - match: off
      control_id: "{control_id}"
      event_type: press
"""
    )


def _input_message() -> hw_messages.ControlInputMessage:
    return hw_messages.control_input_message(
        manager_id="mqtt-main",
        sender_session_id=MANAGER_SESSION,
        device_id="remote-0x0330",
        fingerprint="remote-0x0330",
        control_id="0,0",
        capability_id="button.press",
        event_type="press",
        value={"eventType": "press"},
    )


def _command_message(controller_id: str, image: bytes) -> hw_messages.ControlCommandMessage:
    return hw_messages.control_command_for_capability(
        controller_id=controller_id,
        sender_session_id=CONTROLLER_SESSION,
        ref=CapabilityRef(
            deviceRef=DeviceRef(managerId="mqtt-main", deviceId="remote-0x0330"),
            controlId="0,0",
            capabilityId="raster.bitmap",
        ),
        command_type="set_frame",
        params={
            "image": base64.b64encode(image).decode("ascii"),
            "encoding": "jpeg",
        },
    )


def test_extract_action_values_supports_plain_payload():
    assert _extract_action_values("off") == ["off"]


def test_extract_action_values_supports_json_action_payload():
    assert _extract_action_values(b'{"action":"brightness_step_up","battery":86}') == [
        '{"action":"brightness_step_up","battery":86}',
        "brightness_step_up",
    ]


def test_build_controls_infers_button_and_encoder_controls():
    controls = build_controls(
        [
            RemoteEventMapping(match="off", control_id="0,0", event_type="press"),
            RemoteEventMapping(
                match="brightness_step_up",
                control_id="3,0",
                event_type="rotate",
                direction="clockwise",
            ),
        ]
    )

    assert [control.control_id for control in controls] == ["0,0", "3,0"]
    assert controls[0].kind == "button"
    assert {cap.capability_id for cap in controls[0].input_capabilities} == {
        "button.press",
    }
    assert controls[1].kind == "encoder"
    assert controls[1].input_capabilities[0].capability_id == "encoder.relative"


def test_mapping_builds_control_input_events():
    mapping = RuntimeRemoteMapping(
        match="brightness_step_down",
        control_id="3,0",
        event_type="rotate",
        direction="counterclockwise",
    )
    events = mapping.to_control_input_events()

    assert len(events) == 1
    assert events[0].control_id == "3,0"
    assert events[0].capability_id == "encoder.relative"
    assert events[0].value["direction"] == "counterclockwise"


def test_deduper_suppresses_duplicate_actions_within_window():
    deduper = Deduper(dedupe_ms=500)

    assert deduper.should_emit("off") is True
    assert deduper.should_emit("off") is False


def test_load_remote_devices_reads_yaml_config(
    tmp_path: Path,
):
    (tmp_path / "remote.yml").write_text(
        """
id: remote-0x0330
name: Zigbee remote
remote:
  mqtt:
    topic: zigbee2mqtt/remote/0x0330/action
    dedupe_ms: 300
  events:
    - match: off
      control_id: "0,0"
      event_type: press
    - match: brightness_step_up
      control_id: "3,0"
      event_type: rotate
      direction: clockwise
"""
    )

    devices = load_remote_devices(
        tmp_path,
        default_mqtt=MqttBrokerDefaults(
            hostname="mqtt-default.local",
            port=1883,
            username=None,
            password=None,
        ),
    )

    assert len(devices) == 1
    assert devices[0].id == "remote-0x0330"
    assert devices[0].mqtt_hostname == "mqtt-default.local"
    assert devices[0].mqtt_port == 1883
    assert devices[0].mqtt_topic == "zigbee2mqtt/remote/0x0330/action"
    assert devices[0].dedupe_ms == 300


def test_load_remote_devices_supports_per_device_broker_override(tmp_path: Path):
    (tmp_path / "remote.yml").write_text(
        """
id: remote-0x0330
name: Zigbee remote
remote:
  mqtt:
    hostname: mqtt-z2m.local
    port: 1884
    username: z2m
    password: secret
    topic: zigbee2mqtt/remote/0x0330/action
  events:
    - match: off
      control_id: "0,0"
      event_type: press
"""
    )

    devices = load_remote_devices(
        tmp_path,
        default_mqtt=MqttBrokerDefaults(
            hostname="mqtt-default.local",
            port=1883,
            username=None,
            password=None,
        ),
    )

    assert len(devices) == 1
    assert devices[0].mqtt_hostname == "mqtt-z2m.local"
    assert devices[0].mqtt_port == 1884
    assert devices[0].mqtt_username == "z2m"
    assert devices[0].mqtt_password == "secret"


@pytest.mark.asyncio
async def test_remote_driver_restarts_device_on_config_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    config_path = tmp_path / "remote.yml"
    config_path.write_text(
        """
id: remote-0x0330
name: Zigbee remote
profiles:
  - name: default
    pages:
      - controls: []
remote:
  mqtt:
    hostname: mqtt-a.local
    topic: zigbee2mqtt/remote/0x0330/action
  events:
    - match: off
      control_id: "0,0"
      event_type: press
"""
    )

    started_topics: list[str] = []

    async def fake_device_loop(
        runtime,
        publish_input,
        command_stream,
        manager_id,
        sender_session_id,
    ):
        del publish_input, command_stream, sender_session_id
        assert manager_id == "mqtt-main"
        started_topics.append(runtime.mqtt_topic)
        await anyio.sleep_forever()

    monkeypatch.setattr(
        "deckr.drivers.mqtt._factory.device_loop",
        fake_device_loop,
    )

    async with _deckr() as deckr, anyio.create_task_group() as tg:
        component = RemoteDeviceFactoryComponent(
            deckr.lane("hardware_messages"),
            deckr.state(DEFAULT_LEASE_STATE_STORE_NAME),
            deckr.state(DEFAULT_DISCOVERY_STATE_STORE_NAME),
            manager_id="mqtt-main",
            config_dir=tmp_path,
            default_mqtt=MqttBrokerDefaults(
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
        component._task_group = tg
        component._stop_event = anyio.Event()
        await component._reconcile_devices()

        with anyio.fail_after(2):
            while started_topics != ["zigbee2mqtt/remote/0x0330/action"]:
                await anyio.sleep(0.05)

        config_path.write_text(
            """
id: remote-0x0330
name: Zigbee remote
profiles:
  - name: default
    pages:
      - controls: []
remote:
  mqtt:
    hostname: mqtt-b.local
    topic: zigbee2mqtt/remote/0x0330/action-2
  events:
    - match: off
      control_id: "1,0"
      event_type: press
"""
        )
        await component._reconcile_devices()

        with anyio.fail_after(2):
            while started_topics != [
                "zigbee2mqtt/remote/0x0330/action",
                "zigbee2mqtt/remote/0x0330/action-2",
            ]:
                await anyio.sleep(0.05)

        await component.stop()
        tg.cancel_scope.cancel()


@pytest.mark.asyncio
async def test_reconcile_devices_publishes_aggregate_inventory(tmp_path: Path):
    _write_remote_config(tmp_path / "remote.yml")

    async with _deckr() as deckr:
        component = _factory(deckr, tmp_path)
        await component._reconcile_devices()

        entry = await deckr.state(DEFAULT_DISCOVERY_STATE_STORE_NAME).get(
            hardware_inventory_key("mqtt-main")
        )
        assert entry is not None
        inventory = HardwareInventory.model_validate(entry.value)
        assert set(inventory.devices) == {"remote-0x0330"}
        assert inventory.devices["remote-0x0330"].descriptor.device_id == "remote-0x0330"


@pytest.mark.asyncio
async def test_config_removal_rewrites_inventory(tmp_path: Path):
    config_path = tmp_path / "remote.yml"
    _write_remote_config(config_path)

    async with _deckr() as deckr:
        component = _factory(deckr, tmp_path)
        await component._reconcile_devices()
        config_path.unlink()
        await component._reconcile_devices()

        entry = await deckr.state(DEFAULT_DISCOVERY_STATE_STORE_NAME).get(
            hardware_inventory_key("mqtt-main")
        )
        assert entry is not None
        inventory = HardwareInventory.model_validate(entry.value)
        assert inventory.devices == {}


@pytest.mark.asyncio
async def test_inventory_state_unavailable_keeps_configured_device(tmp_path: Path):
    class UnavailableState:
        async def put(self, *args, **kwargs):
            raise StateUnavailable("temporary substrate outage")

    _write_remote_config(tmp_path / "remote.yml")

    async with _deckr() as deckr:
        component = RemoteDeviceFactoryComponent(
            deckr.lane("hardware_messages"),
            deckr.state(DEFAULT_LEASE_STATE_STORE_NAME),
            UnavailableState(),
            manager_id="mqtt-main",
            config_dir=tmp_path,
            default_mqtt=MqttBrokerDefaults(
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
        await component._reconcile_devices()

    assert "remote-0x0330" in component._devices
    assert component._inventory_revision is None


@pytest.mark.asyncio
async def test_claimed_mqtt_input_is_sent_only_to_claiming_controller(tmp_path: Path):
    _write_remote_config(tmp_path / "remote.yml")

    async with _deckr() as deckr:
        component = _factory(deckr, tmp_path)
        await component._reconcile_devices()
        component._claims["remote-0x0330"] = _claim()
        component._controller_presence_sessions[controller_address("main")] = (
            "controller-session"
        )
        main = _endpoint(deckr, controller_address("main"))
        other = _endpoint(deckr, controller_address("other"), session_id="other-session")

        async with main.subscribe() as main_stream, other.subscribe() as other_stream:
            await component._handle_device_message(
                _input_message()
            )
            received = await main_stream.receive()
            with anyio.move_on_after(0.05) as scope:
                await other_stream.receive()

    assert received.recipient.endpoint == controller_address("main")
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_broker_snapshot_claim_delete_resets_and_drops_input(tmp_path: Path):
    class FakeDevice:
        id = "remote-0x0330"

        def __init__(self) -> None:
            self.clear_raster = AsyncMock()

    _write_remote_config(tmp_path / "remote.yml")

    async with _deckr() as deckr:
        component = _factory(deckr, tmp_path)
        await component._reconcile_devices()
        device = FakeDevice()
        command_send, command_receive = anyio.create_memory_object_stream(
            max_buffer_size=100
        )
        component._command_streams["remote-0x0330"] = command_send
        await _put_controller_presence(deckr)
        claim_key = "claim.device.mqtt-main.remote-0x0330"
        await deckr.state().create(claim_key, _claim())
        await component._reconcile_routing_current_state(reason="test snapshot")
        main = _endpoint(deckr, controller_address("main"))

        async with (
            command_send,
            command_receive,
            main.subscribe() as main_stream,
            anyio.create_task_group() as tg,
        ):
            tg.start_soon(
                _apply_device_commands,
                device,
                command_receive,
                "mqtt-main",
            )
            await deckr.state().delete(claim_key)
            await component._reconcile_routing_current_state(reason="test snapshot")

            await component._handle_device_message(
                _input_message()
            )
            with anyio.move_on_after(0.05) as scope:
                await main_stream.receive()
            tg.cancel_scope.cancel()

    device.clear_raster.assert_not_awaited()
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_prefix_observation_omissions_keep_current_routing(
    tmp_path: Path,
    monkeypatch,
):
    _write_remote_config(tmp_path / "remote.yml")

    async with _deckr() as deckr:
        component = _factory(deckr, tmp_path)
        await component._reconcile_devices()
        await _put_controller_presence(deckr)
        await deckr.state().create(
            "claim.device.mqtt-main.remote-0x0330",
            _claim(),
        )
        await component._reconcile_routing_current_state(reason="initial snapshot")
        assert component._claim_recipient("remote-0x0330") == controller_address("main")

        async def omitted_items(prefix: str = ""):
            del prefix
            return ()

        monkeypatch.setattr(
            deckr.state(DEFAULT_LEASE_STATE_STORE_NAME),
            "items",
            omitted_items,
        )

        await component._reconcile_routing_current_state(reason="omitted snapshot")

        assert component._claim_recipient("remote-0x0330") == controller_address("main")
        assert "remote-0x0330" in component._claims


@pytest.mark.asyncio
async def test_controller_presence_restore_makes_current_claim_routable(tmp_path: Path):
    _write_remote_config(tmp_path / "remote.yml")

    async with _deckr() as deckr:
        component = _factory(deckr, tmp_path)
        await component._reconcile_devices()
        claim_key = "claim.device.mqtt-main.remote-0x0330"
        await deckr.state().create(claim_key, _claim())
        await component._reconcile_routing_current_state(reason="test snapshot")
        assert component._claim_recipient("remote-0x0330") is None

        await _put_controller_presence(deckr)
        await component._reconcile_routing_current_state(reason="test snapshot")
        assert component._claim_recipient("remote-0x0330") == controller_address("main")


@pytest.mark.asyncio
async def test_invalid_claim_payload_is_not_routable(tmp_path: Path):
    class FakeDevice:
        id = "remote-0x0330"

        def __init__(self) -> None:
            self.clear_raster = AsyncMock()

    _write_remote_config(tmp_path / "remote.yml")

    async with _deckr() as deckr:
        component = _factory(deckr, tmp_path)
        await component._reconcile_devices()
        device = FakeDevice()
        command_send, command_receive = anyio.create_memory_object_stream(
            max_buffer_size=100
        )
        component._command_streams["remote-0x0330"] = command_send
        await deckr.state().put(
            "claim.device.mqtt-main.remote-0x0330",
            {
                "claimedByEndpoint": "controller:main",
                "timestamp": datetime.now(UTC).isoformat(),
                "ttlSeconds": 30,
            },
        )
        await _put_controller_presence(deckr)

        async with command_send, command_receive, anyio.create_task_group() as tg:
            tg.start_soon(
                _apply_device_commands,
                device,
                command_receive,
                "mqtt-main",
            )
            await component._reconcile_routing_current_state(reason="test snapshot")
            tg.cancel_scope.cancel()

    assert "remote-0x0330" not in component._claims
    device.clear_raster.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_commands_apply_only_from_claiming_controller(tmp_path: Path):
    class FakeDevice:
        id = "remote-0x0330"

        def __init__(self) -> None:
            self.set_raster_frame = AsyncMock()
            self.clear_raster = AsyncMock()

    _write_remote_config(tmp_path / "remote.yml")

    async with _deckr() as deckr:
        component = _factory(deckr, tmp_path)
        await component._reconcile_devices()
        component._claims["remote-0x0330"] = _claim()
        component._controller_presence_sessions[controller_address("main")] = (
            "controller-session"
        )
        device = FakeDevice()
        command_send, command_receive = anyio.create_memory_object_stream(
            max_buffer_size=100
        )
        component._command_streams["remote-0x0330"] = command_send

        async with command_send, command_receive, anyio.create_task_group() as tg:
            tg.start_soon(
                _apply_device_commands,
                device,
                command_receive,
                "mqtt-main",
            )
            await component._route_command(
                _command_message("other", b"wrong")
            )
            await anyio.sleep(0.05)
            device.set_raster_frame.assert_not_awaited()

            await component._route_command(
                _command_message("main", b"ok")
            )
            with anyio.fail_after(1):
                while device.set_raster_frame.await_count < 1:
                    await anyio.sleep(0.01)
            tg.cancel_scope.cancel()

    device.set_raster_frame.assert_awaited_once_with("0,0", b"ok")
