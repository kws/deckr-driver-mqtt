from pathlib import Path

import anyio
import pytest
from deckr.core.component import RunContext
from deckr.hardware import events as hw_events
from deckr.transports.bus import EventBus

from deckr.drivers.mqtt._factory import (
    Deduper,
    MqttBrokerDefaults,
    RemoteDeviceFactoryComponent,
    RemoteEventMapping,
    RuntimeRemoteMapping,
    _extract_action_values,
    build_slots,
    load_remote_devices,
)


def test_extract_action_values_supports_plain_payload():
    assert _extract_action_values("off") == ["off"]


def test_extract_action_values_supports_json_action_payload():
    assert _extract_action_values(b'{"action":"brightness_step_up","battery":86}') == [
        '{"action":"brightness_step_up","battery":86}',
        "brightness_step_up",
    ]


def test_build_slots_infers_button_and_encoder_controls():
    slots = build_slots(
        [
            RemoteEventMapping(match="off", slot="0,0", gesture="key_up"),
            RemoteEventMapping(
                match="brightness_step_up",
                slot="3,0",
                gesture="encoder_rotate",
                direction="clockwise",
            ),
        ]
    )

    assert [slot.id for slot in slots] == ["0,0", "3,0"]
    assert slots[0].slot_type == "button"
    assert slots[0].gestures == ("key_up",)
    assert slots[1].slot_type == "encoder"
    assert slots[1].gestures == ("encoder_rotate",)


def test_mapping_builds_hardware_event():
    mapping = RuntimeRemoteMapping(
        match="brightness_step_down",
        slot="3,0",
        gesture="encoder_rotate",
        direction="counterclockwise",
    )
    hw_event = mapping.to_hardware_event()

    assert isinstance(hw_event, hw_events.DialRotateMessage)
    assert hw_event.dial_id == "3,0"
    assert hw_event.direction == "counterclockwise"


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
      slot: "0,0"
      gesture: key_up
    - match: brightness_step_up
      slot: "3,0"
      gesture: encoder_rotate
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
      slot: "0,0"
      gesture: key_up
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
      slot: "0,0"
      gesture: key_up
"""
    )

    started_topics: list[str] = []

    async def fake_device_loop(runtime, event_bus, manager_id):
        assert manager_id == "mqtt-main"
        started_topics.append(runtime.mqtt_topic)
        await anyio.sleep_forever()

    monkeypatch.setattr(
        "deckr.drivers.mqtt._factory.device_loop",
        fake_device_loop,
    )

    component = RemoteDeviceFactoryComponent(
        EventBus("hardware_events"),
        manager_id="mqtt-main",
        config_dir=tmp_path,
        default_mqtt=MqttBrokerDefaults(
            hostname="mqtt-default.local",
            port=1883,
            username=None,
            password=None,
        ),
    )
    async with anyio.create_task_group() as tg:
        ctx = RunContext(tg=tg, stopping=anyio.Event())
        tg.start_soon(component.start, ctx)

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
      slot: "1,0"
      gesture: key_up
"""
        )

        with anyio.fail_after(2):
            while started_topics != [
                "zigbee2mqtt/remote/0x0330/action",
                "zigbee2mqtt/remote/0x0330/action-2",
            ]:
                await anyio.sleep(0.05)

        await component.stop()
