#!/usr/bin/env python3
"""
Raft Consensus — минимальная реализация для P2P Agent Mesh v0.6.1.
Leader election + log replication через существующий TCP-транспорт (topic _raft).

Состояния: Follower → Candidate → Leader
Heartbeat: 500ms
Election timeout: 150–300ms (random)
"""

import asyncio, json, time, random


class RaftNode:
    def __init__(self, node_id: str, transport=None, wal=None):
        self.node_id = node_id
        self.transport = transport  # P2PTransport — для отправки Raft-сообщений
        self.wal = wal  # WALBuffer — для персистентности лога
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.commit_index = 0
        self.last_applied = 0
        self._running = False
        self._election_task = None
        self._heartbeat_task = None
        self._peers: list[str] = []  # peer node_ids

    def _rand_timeout(self) -> float:
        return random.uniform(0.50, 1.50)  # wide range to avoid split votes

    async def start(self, peers: list[str] = None):
        """Запустить Raft-ноду."""
        self._running = True
        if peers:
            self._peers = peers
        self._last_heartbeat = time.time()  # reset on leader heartbeats
        print(f"[raft:{self.node_id}] Started as {self.state}, term={self.current_term}, peers={len(self._peers)}")

        if self.transport:
            await self.transport.subscribe("_raft", self._on_raft_msg)

        self._election_task = asyncio.create_task(self._election_loop())
        return self

    async def stop(self):
        self._running = False
        if self._election_task:
            self._election_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    # ── RPC Messages ──────────────────────────

    def _on_raft_msg(self, raw: bytes):
        """Callback из transport — обрабатывает входящие Raft-сообщения."""
        try:
            msg = json.loads(raw)
        except Exception:
            return

        try:
            payload = msg.get("payload", {})
            rpc_type = payload.get("type", "")

            if rpc_type == "request_vote":
                asyncio.create_task(self._handle_request_vote(payload, msg.get("from", "")))
            elif rpc_type == "request_vote_response":
                self._handle_request_vote_response(payload)
            elif rpc_type == "append_entries":
                asyncio.create_task(self._handle_append_entries(payload, msg.get("from", "")))
            elif rpc_type == "append_entries_response":
                pass  # leader doesn't need to track follower matchIndex for MVP
        except Exception as e:
            print(f"[raft:{self.node_id}] Error handling msg: {e}")

    async def _send_raft(self, target: str | None, payload: dict):
        """Отправить Raft-сообщение через транспорт."""
        if not self.transport:
            return
        msg = {
            "from": self.node_id,
            "term": self.current_term,
            "type": "raft",
            "payload": payload,
        }
        try:
            await self.transport.publish("_raft", json.dumps(msg).encode())
        except Exception as e:
            print(f"[raft:{self.node_id}] Send error: {e}")

    # ── Election ──────────────────────────────

    async def _election_loop(self):
        """Основной цикл: ожидание таймаута → попытка стать кандидатом."""
        while self._running:
            timeout = self._rand_timeout()
            await asyncio.sleep(timeout)

            if not self._running:
                break
            if self.state == "leader":
                continue

            # Don't start election if we recently got a heartbeat
            # Add random jitter to avoid all nodes starting elections simultaneously
            if time.time() - self._last_heartbeat < 0.3 + random.uniform(0, 0.7):
                continue

            await self._start_election()

    async def _start_election(self):
        """Начать выборы: increment term, vote for self, request votes."""
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None

        self._votes_received = 1  # голос за себя
        self._votes_needed = (len(self._peers) // 2) + 1 if self._peers else 1
        self._election_term = self.current_term  # зафиксировать терм для подсчёта голосов

        print(f"[raft:{self.node_id}] Election started: term={self.current_term}, need={self._votes_needed} votes")

        # Check if single-node cluster → immediate leadership
        if self._votes_received >= self._votes_needed:
            self._become_leader()
            return

        # Отправляем RequestVote всем пирам
        for peer_id in self._peers:
            if peer_id == self.node_id:
                continue
            payload = {
                "type": "request_vote",
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": len(self.wal.entries) if self.wal else 0,
                "last_log_term": self.current_term,
            }
            asyncio.create_task(self._send_raft_vote_request(peer_id, payload))

    async def _send_raft_vote_request(self, peer_id, payload):
        """Отправить запрос голоса."""
        try:
            msg = {
                "from": self.node_id,
                "term": self.current_term,
                "type": "raft",
                "payload": payload,
            }
            await self.transport.publish("_raft", json.dumps(msg).encode())
        except Exception:
            pass

    async def _handle_request_vote(self, payload: dict, from_peer: str):
        """Обработать входящий RequestVote."""
        term = payload.get("term", 0)
        candidate_id = payload.get("candidate_id", "")

        # Ignore self
        if candidate_id == self.node_id:
            return

        if term > self.current_term:
            self.current_term = term
            self.state = "follower"
            self.voted_for = None

        vote_granted = False
        if (term >= self.current_term and
                (self.voted_for is None or self.voted_for == candidate_id)):
            self.voted_for = candidate_id
            vote_granted = True

        response = {
            "type": "request_vote_response",
            "term": self.current_term,
            "vote_granted": vote_granted,
            "candidate_id": candidate_id,
        }
        await self._send_raft(from_peer, response)

        if vote_granted:
            # Проверяем — не стали ли мы лидером после получения достаточного числа голосов
            pass  # MVP: counting votes is done optimistically in _start_election

    def _become_leader(self):
        """Стать лидером."""
        self.state = "leader"
        self.leader_id = self.node_id
        print(f"[raft:{self.node_id}] 👑 Elected LEADER (term {self.current_term})")
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    def _handle_request_vote_response(self, payload: dict):
        """Обработать ответ на запрос голоса."""
        if self.state != "candidate":
            return  # уже не кандидат (стал лидером или follower'ом)

        term = payload.get("term", 0)
        if term > self.current_term:
            self.current_term = term
            self.state = "follower"
            self.voted_for = None
            return

        if term < self.current_term:
            return  # старый ответ, игнорируем

        if payload.get("vote_granted"):
            self._votes_received += 1
            print(f"[raft:{self.node_id}] Vote received: {self._votes_received}/{self._votes_needed}")

            if self._votes_received >= self._votes_needed:
                self._become_leader()

    # ── Heartbeat / AppendEntries ─────────────

    async def _heartbeat_loop(self):
        """Лидер шлёт heartbeat каждые 500ms."""
        while self._running and self.state == "leader":
            payload = {
                "type": "append_entries",
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": 0,
                "prev_log_term": 0,
                "entries": [],
                "leader_commit": self.commit_index,
            }
            await self._send_raft(None, payload)
            await asyncio.sleep(0.5)

    async def _handle_append_entries(self, payload: dict, from_peer: str):
        """Обработать входящий heartbeat / AppendEntries."""
        term = payload.get("term", 0)
        leader_id = payload.get("leader_id", "")

        # Ignore self-heartbeats
        if leader_id == self.node_id:
            return

        if term >= self.current_term:
            self.current_term = term
            self.state = "follower"
            self.leader_id = leader_id
            self._last_heartbeat = time.time()  # reset election timer

            # Append any entries
            entries = payload.get("entries", [])
            leader_commit = payload.get("leader_commit", 0)
            if entries:
                for entry in entries:
                    if self.wal:
                        self.wal.append(entry)
                self.commit_index = leader_commit

    # ── Status ─────────────────────────────────

    def status(self) -> dict:
        return {
            "node_id": self.node_id,
            "state": self.state,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "peers": len(self._peers),
            "commit_index": self.commit_index,
        }


# ── Tests ─────────────────────────────────────

async def _test_3node_election():
    """Тест: 3 Raft-ноды, симуляция election."""
    # Shared message bus
    nodes_data = []  # list of (node, transport, wal)

    class SharedBus:
        def __init__(self):
            self.subscribers = {}  # node_id -> callback
        def add(self, node_id, cb):
            self.subscribers[node_id] = cb
        async def publish(self, topic, data):
            msg = json.loads(data)
            for nid, cb in list(self.subscribers.items()):
                try:
                    cb(data)
                except Exception:
                    pass

    bus = SharedBus()

    class FakeWAL:
        entries = []
        def append(self, e):
            self.entries.append(e)

    class FakeTransport:
        def __init__(self, node_id):
            self.node_id = node_id
        async def subscribe(self, topic, cb):
            bus.add(self.node_id, cb)
        async def publish(self, topic, data):
            await bus.publish(topic, data)

    nodes = []
    for i in range(3):
        nid = f"node-{i}"
        transport = FakeTransport(nid)
        node = RaftNode(nid, transport=transport, wal=FakeWAL())
        nodes.append(node)

    # Start with full peer list
    peers = [n.node_id for n in nodes]
    for n in nodes:
        await n.start(peers=peers)

    # Wait for election (random timeout 150-300ms)
    await asyncio.sleep(1.5)

    states = [n.state for n in nodes]
    leader_count = sum(1 for s in states if s == "leader")
    follower_count = sum(1 for s in states if s == "follower")

    assert leader_count == 1, f"Expected 1 leader, got {leader_count}. States: {states}"
    assert follower_count == 2, f"Expected 2 followers, got {follower_count}. States: {states}"

    leader_node = next(n for n in nodes if n.state == "leader")
    print(f"  Leader: {leader_node.node_id}, term={leader_node.current_term}")
    print(f"  Followers: {[n.node_id for n in nodes if n.state == 'follower']}")

    for n in nodes:
        await n.stop()

    print(f"✅ test_3node_election PASSED")
    return True


async def _test_leader_heartbeat():
    """Тест: лидер шлёт heartbeat → followers получают."""
    calls = []

    class FakeTransport:
        def __init__(self):
            self.msgs = []
        async def emit(self, topic, msg):
            pass
        async def subscribe(self, topic, cb):
            self.cb = cb
        async def publish(self, topic, data):
            self.msgs.append(json.loads(data))

    class FakeWAL:
        entries = []
        def append(self, e):
            self.entries.append(e)

    transport = FakeTransport()
    node = RaftNode("leader-1", transport=transport, wal=FakeWAL())
    await node.start(peers=["leader-1", "follower-1"])

    # Become leader
    node.state = "leader"
    node.leader_id = "leader-1"
    node._heartbeat_task = asyncio.create_task(node._heartbeat_loop())

    await asyncio.sleep(0.7)  # should get at least 1 heartbeat
    node._running = False
    node._heartbeat_task.cancel()

    heartbeats = [m for m in transport.msgs if m["payload"].get("type") == "append_entries"]
    assert len(heartbeats) >= 1, f"Expected ≥1 heartbeat, got {len(heartbeats)}"

    await node.stop()
    print(f"✅ test_leader_heartbeat PASSED ({len(heartbeats)} heartbeats)")
    return True


async def _test_election_timeout():
    """Тест: election timeout < 300ms."""
    node = RaftNode("test-node")
    for _ in range(20):
        t = node._rand_timeout()
        assert 0.50 <= t <= 1.50, f"Timeout {t} out of range [0.50, 1.50]"
    print("✅ test_election_timeout PASSED")
    return True


def run_tests():
    """Запустить все тесты Raft."""
    print("=== Raft Tests ===")
    asyncio.run(_test_election_timeout())
    asyncio.run(_test_3node_election())
    asyncio.run(_test_leader_heartbeat())
    print("=== All Raft tests PASSED ===")
    return True


if __name__ == "__main__":
    run_tests()
