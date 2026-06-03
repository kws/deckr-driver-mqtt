from __future__ import annotations

import io
import json
from datetime import datetime

import deckr.drivers.mqtt as mqtt_package
from deckr.drivers.mqtt._actions_cli import (
    InspectState,
    action_values_for_observation,
    parse_ha_discovery_trigger,
    process_inspect_message,
    render_device_list,
)
from deckr.drivers.mqtt._zigbee2mqtt import (
    parse_bridge_device_metadata,
    parse_bridge_devices,
    render_inferred_controls,
    zigbee2mqtt_device_topic,
)

PAULMANN_50141_ACTIONS = [
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


def test_package_exports_component_only() -> None:
    assert mqtt_package.__all__ == ["component"]
    assert hasattr(mqtt_package, "component")
    assert not hasattr(mqtt_package, "driver_factory")


def test_zigbee2mqtt_device_topic_supports_slash_friendly_names() -> None:
    assert (
        zigbee2mqtt_device_topic("zigbee2mqtt", "remote/0x0330")
        == "zigbee2mqtt/remote/0x0330"
    )


def test_parse_bridge_devices_extracts_metadata_and_actions() -> None:
    payload = json.dumps(
        [
            {
                "ieee_address": "0xffffaa6712730330",
                "friendly_name": "remote/0x0330",
                "type": "EndDevice",
                "supported": True,
                "disabled": False,
                "power_source": "Battery",
                "model_id": "50141",
                "interview_state": "SUCCESSFUL",
                "definition": {
                    "vendor": "Paulmann",
                    "model": "501.41",
                    "description": "Remote control Smart Home Zigbee 3.0 White",
                    "exposes": [
                        {"type": "numeric", "property": "battery"},
                        {
                            "type": "enum",
                            "property": "action",
                            "values": PAULMANN_50141_ACTIONS,
                        },
                        {"type": "numeric", "property": "linkquality"},
                    ],
                },
            },
            {
                "friendly_name": "unsupported/thing",
                "definition": None,
            },
        ]
    )

    devices = parse_bridge_devices(payload, base_topic="zigbee2mqtt")

    assert len(devices) == 2
    assert devices[0].friendly_name == "remote/0x0330"
    assert devices[0].topic == "zigbee2mqtt/remote/0x0330"
    assert devices[0].device_id == "z2m.0xffffaa6712730330"
    assert devices[0].fingerprint == "zigbee2mqtt:0xffffaa6712730330"
    assert devices[0].vendor == "Paulmann"
    assert devices[0].model == "501.41"
    assert devices[0].model_id == "50141"
    assert devices[0].power_source == "Battery"
    assert devices[0].exposes == ("battery", "action", "linkquality")
    assert devices[0].actions == tuple(PAULMANN_50141_ACTIONS)
    assert devices[1].friendly_name == "unsupported/thing"
    assert devices[1].actions == ()


def test_parse_bridge_device_metadata_preserves_zigbee2mqtt_context() -> None:
    payload = json.dumps(
        [
            {
                "ieee_address": "0x0017880108758e62",
                "type": "EndDevice",
                "network_address": 12345,
                "supported": True,
                "disabled": False,
                "friendly_name": "switch/huedimmer/0x8e62",
                "description": "Bedroom dimmer",
                "power_source": "Battery",
                "date_code": "20170908",
                "model_id": "RWL021",
                "interview_state": "SUCCESSFUL",
                "endpoints": {
                    "1": {
                        "bindings": [{"cluster": "genOnOff", "target": "coordinator"}],
                        "configured_reportings": [],
                        "clusters": {
                            "input": ["genBasic", "genPowerCfg"],
                            "output": ["genOnOff", "genLevelCtrl"],
                        },
                    }
                },
                "definition": {
                    "source": "native",
                    "vendor": "Philips",
                    "model": "324131092621",
                    "description": "Hue dimmer switch",
                    "options": [
                        {
                            "type": "binary",
                            "property": "legacy",
                            "name": "legacy",
                        }
                    ],
                    "exposes": [
                        {"type": "numeric", "property": "battery"},
                        {"type": "numeric", "property": "action_duration"},
                        {
                            "type": "enum",
                            "property": "action",
                            "values": ["on_press", "on_hold", "on_hold_release"],
                        },
                    ],
                },
            }
        ]
    )

    metadata = parse_bridge_device_metadata(payload)

    assert metadata[0]["friendly_name"] == "switch/huedimmer/0x8e62"
    assert metadata[0]["topic"] == "zigbee2mqtt/switch/huedimmer/0x8e62"
    assert metadata[0]["ieee_address"] == "0x0017880108758e62"
    assert metadata[0]["type"] == "EndDevice"
    assert metadata[0]["power_source"] == "Battery"
    assert metadata[0]["definition"]["source"] == "native"
    assert metadata[0]["definition"]["model"] == "324131092621"
    assert metadata[0]["definition"]["expose_names"] == [
        "battery",
        "action_duration",
        "action",
    ]
    assert metadata[0]["definition"]["actions"] == [
        "on_press",
        "on_hold",
        "on_hold_release",
    ]
    assert metadata[0]["definition"]["option_names"] == ["legacy"]
    assert metadata[0]["endpoints"]["1"]["clusters"]["output"] == [
        "genOnOff",
        "genLevelCtrl",
    ]


def test_parse_ha_discovery_trigger_extracts_device_automation() -> None:
    trigger = parse_ha_discovery_trigger(
        "homeassistant/device_automation/node/action_off/config",
        json.dumps(
            {
                "automation_type": "trigger",
                "topic": "zigbee2mqtt/remote/0x0330",
                "payload": "off",
                "type": "action",
                "subtype": "off",
            }
        ),
    )

    assert trigger is not None
    assert trigger.topic == "zigbee2mqtt/remote/0x0330"
    assert trigger.payload == "off"
    assert trigger.trigger_type == "action"
    assert trigger.subtype == "off"


def test_action_values_for_observation_prefers_json_action_property() -> None:
    assert action_values_for_observation('{"action":"off","battery":86}') == ("off",)
    assert action_values_for_observation("off") == ("off",)
    assert action_values_for_observation('"off"') == ("off",)
    assert action_values_for_observation('{"action":""}') == ()
    assert action_values_for_observation(
        '{"action":""}',
        include_empty=True,
    ) == ("",)


def test_process_inspect_message_prints_unique_actions_and_empty_when_enabled() -> None:
    out = io.StringIO()
    state = InspectState()
    timestamp = datetime(2026, 5, 6, 12, 30, 0)

    for _index in range(2):
        process_inspect_message(
            topic="zigbee2mqtt/remote/0x0330",
            payload='{"action":"off","battery":86}',
            device_topic="zigbee2mqtt/remote/0x0330",
            ha_discovery_topic="homeassistant",
            unique=True,
            include_empty=False,
            state=state,
            out=out,
            timestamp=timestamp,
        )
    process_inspect_message(
        topic="zigbee2mqtt/remote/0x0330",
        payload='{"action":""}',
        device_topic="zigbee2mqtt/remote/0x0330",
        ha_discovery_topic="homeassistant",
        unique=True,
        include_empty=False,
        state=state,
        out=out,
        timestamp=timestamp,
    )
    process_inspect_message(
        topic="zigbee2mqtt/remote/0x0330",
        payload='{"action":""}',
        device_topic="zigbee2mqtt/remote/0x0330",
        ha_discovery_topic="homeassistant",
        unique=True,
        include_empty=True,
        state=state,
        out=out,
        timestamp=timestamp,
    )

    lines = out.getvalue().splitlines()
    assert lines == [
        '12:30:00 action=off topic=zigbee2mqtt/remote/0x0330 payload={"action":"off","battery":86}',
        '12:30:00 action= topic=zigbee2mqtt/remote/0x0330 payload={"action":""}',
    ]


def test_process_inspect_message_prints_relevant_ha_discovery_once() -> None:
    out = io.StringIO()
    state = InspectState()
    payload = json.dumps(
        {
            "automation_type": "trigger",
            "topic": "zigbee2mqtt/remote/0x0330",
            "payload": "off",
            "type": "action",
            "subtype": "off",
        }
    )

    for _index in range(2):
        process_inspect_message(
            topic="homeassistant/device_automation/node/action_off/config",
            payload=payload,
            device_topic="zigbee2mqtt/remote/0x0330",
            ha_discovery_topic="homeassistant",
            unique=False,
            include_empty=False,
            state=state,
            out=out,
        )

    assert out.getvalue().splitlines() == [
        "ha-trigger type=action subtype=off payload=off topic=zigbee2mqtt/remote/0x0330"
    ]


def test_render_device_list_supports_json() -> None:
    device = parse_bridge_devices(
        json.dumps([{"friendly_name": "remote/0x0330", "definition": None}])
    )[0]

    rendered = json.loads(render_device_list([device], as_json=True))

    assert rendered[0]["friendly_name"] == "remote/0x0330"
    assert rendered[0]["topic"] == "zigbee2mqtt/remote/0x0330"


def test_render_inferred_controls_outputs_discovery_projection() -> None:
    device = parse_bridge_devices(
        json.dumps(
            [
                {
                    "ieee_address": "0xffffaa6712730330",
                    "friendly_name": "remote/0x0330",
                    "definition": {
                        "exposes": [
                            {
                                "property": "action",
                                "values": ["on", "brightness_move_up", "brightness_stop"],
                            }
                        ]
                    },
                }
            ]
        )
    )[0]

    rendered = json.loads(render_inferred_controls(device))

    assert rendered["device_id"] == "z2m.0xffffaa6712730330"
    assert rendered["fingerprint"] == "zigbee2mqtt:0xffffaa6712730330"
    assert rendered["controls"] == [
        {
            "control_id": "on",
            "capabilities": ["button.press"],
            "actions": [
                {
                    "action": "on",
                    "capability_id": "button.press",
                    "event_type": "press",
                }
            ],
        },
        {
            "control_id": "brightness_up",
            "capabilities": ["button.momentary"],
            "actions": [
                {
                    "action": "brightness_move_up",
                    "capability_id": "button.momentary",
                    "event_type": "down",
                }
            ],
        },
    ]
