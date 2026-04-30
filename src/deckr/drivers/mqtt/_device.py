from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import anyio


@dataclass(frozen=True, slots=True)
class ControlInputEvent:
    control_id: str
    capability_id: str
    event_type: str
    value: dict[str, Any]


class RemoteDevice:
    """Input-only live device backed by a remote MQTT topic."""

    def __init__(self, *, device_id: str, name: str):
        self._device_id = device_id
        self._name = name
        self._hid = f"remote:{device_id}"
        self._event_send, self._event_receive = anyio.create_memory_object_stream[
            ControlInputEvent
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

    async def set_raster_frame(self, control_id: str, image: bytes) -> None:
        return

    async def clear_raster(self, control_id: str) -> None:
        return

    async def emit(self, event: ControlInputEvent) -> None:
        await self._event_send.send(event)

    async def subscribe(self) -> AsyncIterator[ControlInputEvent]:
        try:
            async for event in self._event_receive:
                yield event
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            return

    async def close(self) -> None:
        await self._event_send.aclose()
