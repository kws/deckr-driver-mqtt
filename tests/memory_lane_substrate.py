from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import anyio
from deckr.contracts.lanes import LaneContractRegistry
from deckr.contracts.messages import DeckrMessage, EndpointAddress
from deckr.contracts.models import DeckrModel
from deckr.lanes import (
    ReplyPredicate,
    message_is_deliverable,
    reply_is_accepted,
    validate_message_for_contract,
)
from deckr.state import StateChange, StateConflict, StateEntry, StateStore, state_value


class MemoryLaneSubstrate:
    def __init__(
        self,
        *,
        lane_contracts: LaneContractRegistry,
        buffer_size: int = 100,
    ) -> None:
        self._lane_contracts = lane_contracts
        self._buffer_size = buffer_size
        self._lock = anyio.Lock()
        self._subscribers: dict[
            tuple[str, EndpointAddress],
            set[anyio.abc.ObjectSendStream[DeckrMessage]],
        ] = {}
        self._states: dict[str, MemoryStateStore] = {}

    async def publish(self, message: DeckrMessage) -> None:
        contract = self._lane_contracts.contract_for(message.lane)
        validate_message_for_contract(message, contract)
        async with self._lock:
            subscribers = [
                (endpoint, tuple(streams))
                for (lane, endpoint), streams in self._subscribers.items()
                if lane == message.lane
            ]
        for endpoint, streams in subscribers:
            if not message_is_deliverable(
                message,
                endpoint=endpoint,
                contract=contract,
            ):
                continue
            for stream in streams:
                await stream.send(message)

    async def publish_reply(
        self,
        message: DeckrMessage,
        *,
        request: DeckrMessage,
    ) -> None:
        del request
        await self.publish(message)

    async def request(
        self,
        message: DeckrMessage,
        *,
        timeout: float = 2.0,
        accept: ReplyPredicate | None = None,
    ) -> DeckrMessage:
        async with self.subscribe(message.lane, message.sender) as stream:
            await self.publish(message)
            with anyio.fail_after(timeout):
                while True:
                    reply = await stream.receive()
                    if await reply_is_accepted(reply, request=message, accept=accept):
                        return reply

    @asynccontextmanager
    async def subscribe(
        self,
        lane: str,
        endpoint: EndpointAddress,
    ) -> AsyncIterator[anyio.abc.ObjectReceiveStream[DeckrMessage]]:
        send, receive = anyio.create_memory_object_stream[DeckrMessage](
            max_buffer_size=self._buffer_size
        )
        key = (lane, endpoint)
        async with self._lock:
            self._subscribers.setdefault(key, set()).add(send)
        try:
            yield receive
        finally:
            async with self._lock:
                streams = self._subscribers.get(key)
                if streams is not None:
                    streams.discard(send)
                    if not streams:
                        self._subscribers.pop(key, None)
            await send.aclose()
            await receive.aclose()

    def state(self, name: str) -> StateStore:
        store = self._states.get(name)
        if store is None:
            store = MemoryStateStore(buffer_size=self._buffer_size)
            self._states[name] = store
        return store


class MemoryStateStore:
    def __init__(self, *, buffer_size: int = 100) -> None:
        self._buffer_size = buffer_size
        self._lock = anyio.Lock()
        self._revision = 0
        self._entries: dict[str, StateEntry] = {}
        self._watchers: dict[anyio.abc.ObjectSendStream[StateChange], str] = {}

    async def get(self, key: str) -> StateEntry | None:
        async with self._lock:
            return self._entries.get(key)

    async def items(self, prefix: str = "") -> tuple[StateEntry, ...]:
        async with self._lock:
            return tuple(
                entry
                for key, entry in sorted(self._entries.items())
                if key.startswith(prefix)
            )

    async def put(
        self,
        key: str,
        value: Mapping[str, Any] | DeckrModel,
        *,
        ttl: float | None = None,
    ) -> StateEntry:
        del ttl
        normalized = state_value(value)
        async with self._lock:
            entry = self._next_entry(key, normalized)
            self._entries[key] = entry
            watchers = self._watchers_for(key)
        await self._publish(watchers, StateChange("put", key, entry))
        return entry

    async def create(
        self,
        key: str,
        value: Mapping[str, Any] | DeckrModel,
        *,
        ttl: float | None = None,
    ) -> StateEntry:
        async with self._lock:
            if key in self._entries:
                raise StateConflict(f"State key {key!r} already exists")
        return await self.put(key, value, ttl=ttl)

    async def update(
        self,
        key: str,
        value: Mapping[str, Any] | DeckrModel,
        *,
        revision: int,
        ttl: float | None = None,
    ) -> StateEntry:
        del ttl
        normalized = state_value(value)
        async with self._lock:
            current = self._entries.get(key)
            if current is None or current.revision != revision:
                raise StateConflict(f"State key {key!r} revision changed")
            entry = self._next_entry(key, normalized)
            self._entries[key] = entry
            watchers = self._watchers_for(key)
        await self._publish(watchers, StateChange("put", key, entry))
        return entry

    async def delete(self, key: str, *, revision: int | None = None) -> None:
        async with self._lock:
            current = self._entries.get(key)
            if current is None:
                return
            if revision is not None and current.revision != revision:
                raise StateConflict(f"State key {key!r} revision changed")
            self._entries.pop(key, None)
            watchers = self._watchers_for(key)
        await self._publish(watchers, StateChange("delete", key, None))

    @asynccontextmanager
    async def watch(
        self,
        prefix: str = "",
    ) -> AsyncIterator[anyio.abc.ObjectReceiveStream[StateChange]]:
        send, receive = anyio.create_memory_object_stream[StateChange](
            max_buffer_size=self._buffer_size
        )
        async with self._lock:
            self._watchers[send] = prefix
            snapshot = tuple(
                entry
                for key, entry in sorted(self._entries.items())
                if key.startswith(prefix)
            )
        for entry in snapshot:
            await send.send(StateChange("put", entry.key, entry))
        try:
            yield receive
        finally:
            async with self._lock:
                self._watchers.pop(send, None)
            await send.aclose()
            await receive.aclose()

    def _next_entry(self, key: str, value: Mapping[str, Any]) -> StateEntry:
        self._revision += 1
        return StateEntry(key=key, value=value, revision=self._revision)

    def _watchers_for(
        self, key: str
    ) -> tuple[anyio.abc.ObjectSendStream[StateChange], ...]:
        return tuple(
            stream for stream, prefix in self._watchers.items() if key.startswith(prefix)
        )

    async def _publish(
        self,
        watchers: tuple[anyio.abc.ObjectSendStream[StateChange], ...],
        change: StateChange,
    ) -> None:
        for watcher in watchers:
            await watcher.send(change)
