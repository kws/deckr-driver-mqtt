from __future__ import annotations

import json

import anyio
import deckr.hardware.messages as hw_messages
import pytest
from deckr.beacon import DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME, BeaconDiscovery
from deckr.components import RunContext
from deckr.concord import (
    DEFAULT_CONCORD_CONTRACT_STORE_NAME,
    DEFAULT_CONCORD_TOKEN_STORE_NAME,
    ConcordCoordinator,
    ContractValidityStatus,
)
from deckr.contracts.lanes import CORE_LANE_CONTRACTS, LaneContractRegistry
from deckr.contracts.messages import controller_address, hardware_manager_address
from deckr.hardware import (
    HARDWARE_CLAIM_PROFILE_ID,
    HARDWARE_FEATURE_ID,
    DeviceRef,
    HardwareBeaconPayload,
    HardwareClaimDevice,
    HardwareClaimTerms,
)
from deckr.runtime import Deckr
from memory_lane_substrate import MemoryLaneSubstrate

from deckr.drivers.mqtt._factory import driver_factory
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


def _deckr() -> Deckr:
    registry = LaneContractRegistry(CORE_LANE_CONTRACTS.values())
    return Deckr(
        lane_contracts=registry,
        substrate=MemoryLaneSubstrate(lane_contracts=registry),
    )


def _beacon(deckr: Deckr) -> BeaconDiscovery:
    return BeaconDiscovery(deckr.state(DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME))


def _concord(deckr: Deckr) -> ConcordCoordinator:
    return ConcordCoordinator(
        deckr.state(DEFAULT_CONCORD_CONTRACT_STORE_NAME),
        deckr.state(DEFAULT_CONCORD_TOKEN_STORE_NAME),
    )


def _z2m_device() -> dict:
    return {
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
                    "values": PAULMANN_ACTIONS,
                },
                {"type": "numeric", "property": "linkquality"},
            ],
        },
    }


def _bridge_devices():
    return parse_bridge_devices(json.dumps([_z2m_device()]))


async def _claim(factory, concord: ConcordCoordinator, controller_endpoint):
    runtime = factory._runtime
    assert runtime is not None
    advertisement = runtime.advertisement
    assert advertisement is not None
    device = next(iter(factory._runtimes.values()))
    terms = HardwareClaimTerms(
        claimId="claim-1",
        controllerEndpoint=controller_endpoint.endpoint,
        managerEndpoint=hardware_manager_address("mqtt-main"),
        managerAdvertisementId=advertisement.advertisement_id,
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
    contract = await concord.create_contract(
        (controller_endpoint.endpoint, hardware_manager_address("mqtt-main")),
        contract_id="claim-1",
        profile=HARDWARE_CLAIM_PROFILE_ID,
        terms=terms,
        created_by=controller_endpoint.endpoint,
    )
    await concord.attach(
        contract,
        controller_endpoint.endpoint,
        controller_endpoint.session_id,
    )
    await runtime.reconcile_claims(reason="test")
    return contract


async def test_mqtt_advertises_hardware_and_routes_claimed_input() -> None:
    deckr = _deckr()
    concord = _concord(deckr)
    factory = driver_factory(
        deckr.lane("hardware_messages"),
        _beacon(deckr),
        concord,
        manager_id="mqtt-main",
        config={"labels": {"room": "office"}},
    )
    controller_cm = deckr.lane("hardware_messages").register_endpoint(
        controller_address("controller-main")
    )
    controller_endpoint = await controller_cm.__aenter__()
    try:
        async with anyio.create_task_group() as tg:
            await factory.start(RunContext(tg=tg, stopping=anyio.Event()))
            await factory._reconcile_discovered_devices(_bridge_devices())
            runtime = factory._runtime
            assert runtime is not None

            candidates = await _beacon(deckr).find(HARDWARE_FEATURE_ID)
            payload = HardwareBeaconPayload.model_validate(
                candidates[0].advertisement.payload
            )
            device = next(iter(factory._runtimes.values()))
            assert payload.labels == {"room": "office"}
            assert payload.devices[device.id].descriptor == device.descriptor

            contract = await _claim(factory, concord, controller_endpoint)
            assert (await concord.validate(contract)).status == ContractValidityStatus.VALID

            async with controller_endpoint.subscribe() as stream:
                await factory._handle_mqtt_device_payload(
                    topic=device.topic,
                    payload=json.dumps({"action": "on"}),
                )
                with anyio.fail_after(1):
                    routed = await stream.receive()
            assert routed.recipient.endpoint == controller_endpoint.endpoint
            assert routed.recipient_session_id == controller_endpoint.session_id

            await factory.stop()
            tg.cancel_scope.cancel()
    finally:
        await controller_cm.__aexit__(None, None, None)


async def test_mqtt_authorized_commands_are_rejected_as_unsupported() -> None:
    deckr = _deckr()
    concord = _concord(deckr)
    factory = driver_factory(
        deckr.lane("hardware_messages"),
        _beacon(deckr),
        concord,
        manager_id="mqtt-main",
    )
    controller_cm = deckr.lane("hardware_messages").register_endpoint(
        controller_address("controller-main")
    )
    controller_endpoint = await controller_cm.__aenter__()
    try:
        async with anyio.create_task_group() as tg:
            await factory.start(RunContext(tg=tg, stopping=anyio.Event()))
            await factory._reconcile_discovered_devices(_bridge_devices())
            runtime = factory._runtime
            assert runtime is not None
            device = next(iter(factory._runtimes.values()))
            await _claim(factory, concord, controller_endpoint)

            command = hw_messages.control_command_message(
                controller_id="controller-main",
                sender_session_id=controller_endpoint.session_id,
                manager_id="mqtt-main",
                device_id=device.id,
                control_id="button.1",
                capability_id="button.1.momentary",
                command_type="noop",
            )
            async with controller_endpoint.subscribe() as stream:
                assert not await runtime.handle_command(command)
                with anyio.fail_after(1):
                    rejected = await stream.receive()
            body = hw_messages.hardware_body_from_message(rejected)
            assert isinstance(body, hw_messages.CommandRejectedMessage)
            assert body.reason == "unsupported"

            await factory.stop()
            tg.cancel_scope.cancel()
    finally:
        await controller_cm.__aexit__(None, None, None)
