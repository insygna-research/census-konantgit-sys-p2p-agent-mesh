# Copyright 2026 SNIN Network <snin@v2.site>
# SPDX-License-Identifier: MIT

"""Phase 0 — DHT: Kademlia-style distributed hash table (v0.6.2).

Улучшения v0.6.2:
- 8 Kademlia buckets (3-bit prefix routing)
- self-publish с периодическим refresh (60s)
- find_agents(capability) — поиск агентов по capability
- bridge-independent discovery
"""

import time
from collections import OrderedDict


class DHTStore:
    """Локальный DHT кэш + Kademlia-подобные бакеты."""

    BUCKET_COUNT = 8  # 3 бита → 8 бакетов
    BUCKET_BITS = 3

    def __init__(self, node_id: str, max_keys: int = 1000):
        self.node_id = node_id
        self._cache: OrderedDict[str, dict] = OrderedDict()
        self._max_keys = max_keys
        self._topic = "_dht"
        # Kademlia buckets: bucket[bucket_index] → list of {key, value, ts}
        self._buckets: list[list[dict]] = [[] for _ in range(self.BUCKET_COUNT)]
        self._published_self = False

    def _bucket_index(self, key: str) -> int:
        """Вычислить индекс бакета по первым 3 битам хеша ключа."""
        h = hash(key) & 0xFFFFFFFF
        return (h >> 29) & 0x7  # top 3 bits → 0..7

    # ── Kademlia bucket operations ─────────────

    def _bucket_put(self, key: str, value, ts: float = None):
        """Сохранить в бакет (с удалением дубликатов)."""
        idx = self._bucket_index(key)
        b = self._buckets[idx]
        ts = ts or time.time()

        # Remove existing entry with same key
        b[:] = [e for e in b if e["key"] != key]
        b.append({"key": key, "value": value, "ts": ts})

        # Trim bucket — keep last 20 entries
        if len(b) > 20:
            b[:] = b[-20:]

    def _bucket_get(self, key: str) -> dict | None:
        """Найти в бакете."""
        idx = self._bucket_index(key)
        for entry in self._buckets[idx]:
            if entry["key"] == key:
                return entry["value"]
        return None

    def _bucket_search(self, capability: str) -> list[dict]:
        """Поиск агентов по capability во всех бакетах."""
        results = []
        for bucket in self._buckets:
            for entry in bucket:
                v = entry.get("value", {})
                caps = v.get("capabilities", []) if isinstance(v, dict) else []
                if capability in caps or v.get("agent_id") == capability:
                    results.append({"key": entry["key"], "value": v, "ts": entry["ts"]})
        return results

    def bucket_stats(self) -> dict:
        """Статистика бакетов для /api/dht."""
        return {
            "buckets": self.BUCKET_COUNT,
            "total_entries": sum(len(b) for b in self._buckets),
            "per_bucket": [len(b) for b in self._buckets],
        }

    # ── Core DHT operations ────────────────────

    def handle_message(self, msg: dict) -> str | None:
        """Обработать входящее DHT-сообщение."""
        payload = msg.get("payload", {})
        op = payload.get("op")
        if op == "put":
            key = payload.get("key")
            value = payload.get("value")
            ttl = payload.get("ttl", 86400)
            # Сохраняем в кэш и в бакет
            stored = self._store(key, value, ttl, msg.get("from"))
            self._bucket_put(key, value)
            return stored
        elif op == "get":
            key = payload.get("key")
            return self._lookup(key)
        elif op == "put_agent":
            # Агент публикует свой профиль: {agent_id, did, capabilities, endpoints}
            agent_info = payload.get("value", {})
            agent_id = agent_info.get("agent_id", "")
            if agent_id:
                key = f"agent:{agent_id}"
                self._store(key, agent_info, 300, msg.get("from"))
                self._bucket_put(key, agent_info)
                return key
        return None

    def _store(self, key: str, value, ttl: int, source: str) -> str:
        """Сохранить значение в локальном кэше (репликация)."""
        expires = time.time() + ttl
        entry = {
            "value": value,
            "expires": expires,
            "source": source,
            "stored_at": time.time(),
        }
        self._cache[key] = entry
        self._cache.move_to_end(key)
        while len(self._cache) > self._max_keys:
            self._cache.popitem(last=False)
        return key

    def put(self, key: str, value, ttl: int = 86400) -> dict:
        """Подготовить PUT сообщение для публикации в mesh."""
        return {
            "topic": self._topic,
            "payload": {"op": "put", "key": key, "value": value, "ttl": ttl},
        }

    def put_agent(self, agent_id: str, did: str, capabilities: list[str],
                  endpoints: list[str] = None,
                  reputation: float = 1.0) -> dict:
        """Подготовить публикацию профиля агента в DHT."""
        agent_info = {
            "agent_id": agent_id,
            "did": did,
            "capabilities": capabilities,
            "endpoints": endpoints or [],
            "reputation": reputation,
        }
        return {
            "topic": self._topic,
            "payload": {"op": "put_agent", "key": f"agent:{agent_id}",
                         "value": agent_info, "ttl": 300},
        }

    def get(self, key: str) -> dict | None:
        """Получить значение из локального кэша."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        if entry["expires"] < time.time():
            del self._cache[key]
            return None
        return {"key": key, "value": entry["value"], "source": entry["source"]}

    def find_agents(self, capability: str) -> list[dict]:
        """Найти всех агентов с заданной capability (v0.6.2)."""
        return self._bucket_search(capability)

    def get_all_entries(self) -> list[dict]:
        """Все живые записи в кэше."""
        entries = []
        now = time.time()
        for key, entry in list(self._cache.items()):
            if entry["expires"] >= now:
                entries.append({
                    "key": key,
                    "value": entry["value"],
                    "source": entry.get("source"),
                    "ts": entry.get("stored_at", 0),
                })
        return entries

    def _lookup(self, key: str) -> str | None:
        """Локальный поиск (для GET-запроса от других узлов)."""
        entry = self._cache.get(key)
        if entry and entry["expires"] >= time.time():
            return str(entry["value"])
        return None

    def get_topic(self) -> str:
        return self._topic
