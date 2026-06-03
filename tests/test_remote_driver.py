from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import anyio
import deckr.hardware.messages as hw_messages
import pytest
from deckr.components import (
    ComponentState,
    ReadinessState,
    resolve_component_host_plan,
    start_components,
)
from deckr.concord import ContractValidityStatus
from deckr.contracts.messages import controller_address, hardware_manager_address
from deckr.core.config import ConfigDocument
from deckr.hardware import (
    HARDWARE_CLAIM_PROFILE_ID,
    HARDWARE_FEATURE_ID,
    DeviceRef,
    HardwareBeaconPayload,
    HardwareClaimDevice,
    HardwareClaimTerms,
)
from message_bus_mocks import mock_deckr

from deckr.drivers.mqtt import _factory as factory_module
from deckr.drivers.mqtt._zigbee2mqtt import parse_bridge_devices

pytestmark = pytest.mark.asyncio

PAULMANN_ACTIONS = [
    "on",
    "off",
    "brightness_move_up",
    "brightness_move_down",
    "brightness_stop",
    "brightness_step_up",
    "brightness_step_down",
]


class RecordingMqttClient:
    def __init__(self) -> None:
        self.subscribed: list[tuple[str, int]] = []
        self.unsubscribed: list[str] = []

    async def subscribe(self, topic: str, *, qos: int) -> None:
        self.subscribed.append((topic, qos))

    async def unsubscribe(self, topic: str) -> None:
        self.unsubscribed.append(topic)


def _document(config: dict[str, Any] | None = None) -> ConfigDocument:
    return ConfigDocument(
        raw={
            "deckr": {
                "components": {
                    "instances": {
                        "mqtt": {
                            "component": "dev.deckr.hardware.mqtt",
                            "instance_id": "main",
                            "endpoints": {"hardware_manager": "mqtt-main"},
                            "config": config or {},
                        }
                    }
                }
            }
        },
        source_path=None,
        base_dir=Path.cwd(),
    )


def _z2m_device(*, friendly_name: str = "remote/0x0330") -> dict[str, Any]:
    return {
        "ieee_address": "0xffffaa6712730330",
        "friendly_name": friendly_name,
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
                    "values": PAULMANN_ACTIONS,
                },
                {"type": "numeric", "property": "linkquality"},
            ],
        },
    }


def _bridge_devices(*devices: dict[str, Any]):
    return parse_bridge_devices(json.dumps(list(devices or (_z2m_device(),))))


@asynccontextmanager
async def _running_component(config: dict[str, Any] | None = None):
    plan = resolve_component_host_plan(
        _document(config),
        definitions={"dev.deckr.hardware.mqtt": factory_module.component},
    )
    deckr_cm = mock_deckr(
        lane_contracts=plan.lane_contracts,
        lanes=plan.lane_names,
    )
    deckr = await deckr_cm.__aenter__()
    component_cm = start_components(deckr, plan)
    component_host = await component_cm.__aenter__()
    factory = component_host.components[0]
    try:
        await component_host.component_manager.wait_for_state(
            "dev.deckr.hardware.mqtt:main",
            ComponentState.RUNNING,
        )
        with anyio.fail_after(1):
            while factory._runtime is None:
                await anyio.sleep(0.01)
        yield deckr, component_host, factory
    finally:
        await component_cm.__aexit__(None, None, None)
        await deckr_cm.__aexit__(None, None, None)


async def _wait_for_readiness(
    component_host,
    readiness_state: ReadinessState,
) -> None:
    with anyio.fail_after(1):
        while True:
            status = component_host.component_manager.get_component_status(
                "dev.deckr.hardware.mqtt:main"
            )
            if status is not None and status.readiness_state == readiness_state:
                return
            await anyio.sleep(0.01)


async def _wait_for_hardware_payload(deckr, *, device_ids: set[str] | None = None):
    with anyio.fail_after(1):
        while True:
            candidates = deckr.beacon.candidates(HARDWARE_FEATURE_ID)
            if candidates:
                payload = HardwareBeaconPayload.model_validate(
                    candidates[0].advertisement.payload
                )
                if device_ids is None or set(payload.devices) == device_ids:
                    return payload
            await anyio.sleep(0.01)


async def _claim(factory, concord, controller_endpoint):
    runtime = factory._runtime
    assert runtime is not None
    device = next(iter(factory._runtimes.values()))
    terms = HardwareClaimTerms(
        claimId="claim-1",
        controllerEndpoint=controller_endpoint.address,
        managerEndpoint=hardware_manager_address("mqtt-main"),
        devices=(
            HardwareClaimDevice(
                deviceRef=DeviceRef(
                    managerId="mqtt-main",
                    deviceId=device.id,
                    fingerprint=device.fingerprint,
                ),
                instanceCount=1,
            ),
        ),
    )
    contract = await concord._create_contract(
        (controller_endpoint.address, hardware_manager_address("mqtt-main")),
        contract_id="claim-1",
        profile=HARDWARE_CLAIM_PROFILE_ID,
        terms=terms,
        created_by=controller_endpoint.address,
    )
    await concord._attach(
        contract,
        controller_endpoint.address,
        controller_endpoint.session_id,
    )
    await concord.wait_current()
    await runtime.reconcile_claims(reason="test")
    return contract


async def test_component_starts_hosted_unready_without_broker_and_advertises_labels():
    async with _running_component(
        {"labels": {"room": "office"}},
    ) as (deckr, component_host, factory):
        await _wait_for_readiness(component_host, ReadinessState.UNREADY)
        status = component_host.component_manager.get_component_status(
            "dev.deckr.hardware.mqtt:main"
        )
        assert status is not None
        assert status.readiness_reasons == ("missing_broker_hostname",)

        runtime = factory._runtime
        assert runtime is not None
        assert runtime.endpoint.address == hardware_manager_address("mqtt-main")
        assert runtime.endpoint.metadata["endpointSlot"] == "hardware_manager"

        await factory._reconcile_discovered_devices(_bridge_devices())
        payload = await _wait_for_hardware_payload(
            deckr,
            device_ids={"z2m.0xffffaa6712730330"},
        )
        device = next(iter(factory._runtimes.values()))
        assert payload.labels == {"room": "office"}
        assert payload.devices[device.id].descriptor == device.descriptor


async def test_claimed_input_is_routed_and_authorized_commands_are_unsupported():
    async with _running_component() as (deckr, _host, factory):
        await factory._reconcile_discovered_devices(_bridge_devices())
        runtime = factory._runtime
        assert runtime is not None
        device = next(iter(factory._runtimes.values()))

        async with deckr.endpoint(controller_address("controller-main")) as controller:
            contract = await _claim(factory, deckr.concord, controller)
            assert (await deckr.concord._validate(contract)).status == (
                ContractValidityStatus.VALID
            )

            deckr._message_bus.publish.reset_mock()
            await factory._handle_mqtt_device_payload(
                topic=device.topic,
                payload=json.dumps({"action": "on"}),
            )
            routed = deckr._message_bus.publish.call_args.args[0]
            assert routed.recipient.endpoint == controller.address
            assert routed.recipient_session_id == controller.session_id
            body = hw_messages.hardware_body_from_message(routed)
            assert isinstance(body, hw_messages.ControlInputMessage)
            assert body.control_id == "on"
            assert body.event_type == "press"

            command = hw_messages.control_command_message(
                controller_id="controller-main",
                sender_session_id=controller.session_id,
                manager_id="mqtt-main",
                device_id=device.id,
                control_id="on",
                capability_id="button.press",
                command_type="noop",
            )
            deckr._message_bus.publish_reply.reset_mock()
            assert not await runtime.handle_command(command)
            rejected = deckr._message_bus.publish_reply.call_args.args[0]
            rejection = hw_messages.hardware_body_from_message(rejected)
            assert isinstance(rejection, hw_messages.CommandRejectedMessage)
            assert rejection.reason == "unsupported"


async def test_device_removal_cancels_live_claim_contract():
    async with _running_component() as (deckr, _host, factory):
        await factory._reconcile_discovered_devices(_bridge_devices())
        runtime = factory._runtime
        assert runtime is not None

        async with deckr.endpoint(controller_address("controller-main")) as controller:
            contract = await _claim(factory, deckr.concord, controller)
            assert (await deckr.concord._validate(contract)).status == (
                ContractValidityStatus.VALID
            )

            await factory._reconcile_discovered_devices(())
            assert (await deckr.concord._validate(contract)).status == (
                ContractValidityStatus.CANCELLED
            )
            assert runtime.live_claims == ()
            payload = await _wait_for_hardware_payload(deckr, device_ids=set())
            assert payload.devices == {}


async def test_bridge_payload_reconciles_mqtt_subscriptions_and_ignores_invalid_payload():
    async with _running_component() as (_deckr, _host, factory):
        client = RecordingMqttClient()
        subscribed_topics: set[str] = set()

        await factory._handle_bridge_devices_payload(
            json.dumps([_z2m_device()]),
            client=client,
            subscribed_device_topics=subscribed_topics,
        )
        assert client.subscribed == [("zigbee2mqtt/remote/0x0330", 2)]
        assert subscribed_topics == {"zigbee2mqtt/remote/0x0330"}

        await factory._handle_bridge_devices_payload(
            json.dumps([{"friendly_name": "ignored", "definition": None}]),
            client=client,
            subscribed_device_topics=subscribed_topics,
        )
        assert client.unsubscribed == ["zigbee2mqtt/remote/0x0330"]
        assert subscribed_topics == set()

        await factory._handle_bridge_devices_payload(
            "{}",
            client=client,
            subscribed_device_topics=subscribed_topics,
        )
        assert client.subscribed == [("zigbee2mqtt/remote/0x0330", 2)]
        assert client.unsubscribed == ["zigbee2mqtt/remote/0x0330"]


async def test_dedupe_and_directional_stop_mapping_are_per_device():
    runtime = factory_module._runtime_from_zigbee2mqtt_device(
        _bridge_devices()[0],
        dedupe_ms=50,
    )
    assert runtime is not None

    assert [event.event_type for event in runtime.events_for_payload('"on"')] == [
        "press"
    ]
    assert runtime.events_for_payload('"on"') == ()
    await anyio.sleep(0.06)
    assert [event.event_type for event in runtime.events_for_payload('"on"')] == [
        "press"
    ]

    down = runtime.events_for_payload(json.dumps({"action": "brightness_move_up"}))
    assert [(event.control_id, event.event_type) for event in down] == [
        ("brightness_up", "down")
    ]
    up = runtime.events_for_payload(json.dumps({"action": "brightness_stop"}))
    assert [(event.control_id, event.event_type) for event in up] == [
        ("brightness_up", "up")
    ]
    assert runtime.events_for_payload(json.dumps({"action": "brightness_stop"})) == ()

    down = runtime.events_for_payload(json.dumps({"action": "brightness_move_down"}))
    up = runtime.events_for_payload(json.dumps({"action": "brightness_stop"}))
    assert [(event.control_id, event.event_type) for event in down + up] == [
        ("brightness_down", "down"),
        ("brightness_down", "up"),
    ]
