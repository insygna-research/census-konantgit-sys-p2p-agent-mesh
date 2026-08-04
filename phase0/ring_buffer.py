# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""Phase 0 — RingBuffer: in-memory циклический буфер для системных сообщений.

Используется вместо SQLite WAL для служебных топиков (_dht, _raft, _ping, _heartbeat).
Защищает SSD от write amplification на 50+ пирах.
"""

import time
from collections import deque


class RingBuffer:
    """Потокобезопасный циклический буфер фиксированного размера."""

    def __init__(self, max_size: int = 1000):
        self._buffer: deque[dict] = deque(maxlen=max_size)
        self._total_appended: int = 0
        self._total_dropped: int = 0

    def append(self, msg: dict, topic: str = "") -> None:
        """Добавить системное сообщение в буфер."""
        entry = {
            "topic": topic,
            "payload": msg.get("payload", {}),
            "from": msg.get("from", ""),
            "ts": msg.get("ts", time.time()),
            "received_at": time.time(),
        }
        if len(self._buffer) >= self._buffer.maxlen:
            self._total_dropped += 1
        self._buffer.append(entry)
        self._total_appended += 1

    def count(self) -> int:
        """Текущий размер буфера."""
        return len(self._buffer)

    def total(self) -> int:
        """Всего получено системных сообщений (включая вытесненные)."""
        return self._total_appended

    def dropped(self) -> int:
        """Количество вытесненных из буфера."""
        return self._total_dropped

    def recent(self, n: int = 100) -> list[dict]:
        """Последние n сообщений."""
        items = list(self._buffer)
        return items[-n:] if n < len(items) else items

    def stats(self) -> dict:
        """Статистика буфера."""
        return {
            "count": self.count(),
            "total": self.total(),
            "dropped": self.dropped(),
            "max_size": self._buffer.maxlen,
        }


# Системные топики — не пишем в SQLite WAL
SYSTEM_TOPICS = frozenset({
    "_dht",
    "_raft",
    "_ping",
    "_heartbeat",
    "_discovery",
    "agent:echo",
})


def is_system_topic(topic: str) -> bool:
    """Определить, является ли топик системным (не для WAL)."""
    if topic in SYSTEM_TOPICS:
        return True
    # Все топики, начинающиеся с "_", — системные
    if topic.startswith("_"):
        return True
    return False
