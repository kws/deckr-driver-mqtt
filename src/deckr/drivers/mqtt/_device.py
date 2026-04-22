from __future__ import annotations

from collections.abc import AsyncIterator

import anyio

from deckr.hardware import events as hw_events


class RemoteDevice:
    """Input-only HWDevice backed by a remote MQTT topic."""

    def __init__(self, *, device_id: str, name: str, slots: list[hw_events.HWSlot]):
        self._device_id = device_id
        self._name = name
        self._hid = f"remote:{device_id}"
        self._slots = slots
        self._event_send, self._event_receive = anyio.create_memory_object_stream[
            hw_events.HardwareEvent
        ](max_buffer_size=100)

    @property
    def id(self) -> str:
        return self._device_id

    @property
    def name(self) -> str:
        return self._name

    @property
    def hid(self) -> str:
        return self._hid

    @property
    def slots(self) -> list[hw_events.HWSlot]:
        return self._slots

    async def set_image(self, slot_id: str, image: bytes) -> None:
        return

    async def clear_slot(self, slot_id: str) -> None:
        return

    async def sleep_screen(self) -> None:
        return

    async def wake_screen(self) -> None:
        return

    async def emit(self, event: hw_events.HardwareEvent) -> None:
        await self._event_send.send(event)

    async def subscribe(self) -> AsyncIterator[hw_events.HardwareEvent]:
        try:
            async for event in self._event_receive:
                yield event
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            return

    async def close(self) -> None:
        await self._event_send.aclose()
