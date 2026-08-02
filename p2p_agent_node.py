#!/usr/bin/env python3
"""P2P Agent Node — запускает узел в mesh сети.

Использование:
    python3 p2p_agent_node.py --name cryter --port 9097 --dash-port PORT

Теперь подписывает сообщения через Nostr-ключи (nsec из Agent Registry).
"""

import argparse
import asyncio
import json
import sys
import os
import time
import hashlib
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from phase0.transport import P2PTransport
import sqlite3

CHRONO_DB = "/home/agent/data/sites/chrono/chrono.db"
CHRONO_DB_ENC = "/home/agent/data/sites/chrono/chrono.db"  # same for now


def load_agent_keys(agent_id: str) -> dict:
    """Загрузить ключи агента из Agent Registry (chrono.db).

    Расшифровывает nsec через мастер-ключ Phase 0.
    В Phase 1 — через пароль пользователя.
    """
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    import base64

    MASTER_SEED = "snin-agent-vault-master-seed-2026"
    salt = b"snin_vault_salt_001"
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000)
    master_key = base64.urlsafe_b64encode(kdf.derive(MASTER_SEED.encode()))
    cipher = Fernet(master_key)

    db = sqlite3.connect(CHRONO_DB)
    row = db.execute(
        "SELECT nsec_enc, hex_pub, npub FROM agent_registry WHERE agent_id = ?",
        (agent_id,)
    ).fetchone()
    db.close()

    if not row:
        print(f"[{agent_id}] ERROR: not found in agent_registry")
        sys.exit(1)

    nsec_enc, hex_pub, npub = row
    if not nsec_enc:
        print(f"[{agent_id}] ERROR: no nsec_enc in registry")
        sys.exit(1)

    try:
        nsec_bytes = cipher.decrypt(nsec_enc.encode())
        nsec_str = nsec_bytes.decode().strip()
    except Exception as e:
        print(f"[{agent_id}] ERROR decrypting nsec: {e}")
        sys.exit(1)

    return {
        "nsec": nsec_str,
        "hex_pub": hex_pub,
        "npub": npub,
    }


def sign_nostr_event(private_key_hex: str, content: dict, kind: int = 39000) -> dict:
    """Подписать содержимое как Nostr-событие через nostr_protocol."""
    from nostr_protocol import Keys, EventBuilder, Kind
    import time

    keys = Keys.parse(private_key_hex)
    pubkey = keys.public_key()
    content_str = json.dumps(content, ensure_ascii=False)
    created_at = int(time.time())

    builder = EventBuilder(Kind(kind), content_str, [])
    unsigned = builder.to_unsigned_event(pubkey)
    signed = unsigned.sign(keys)

    return json.loads(signed.as_json())


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True, help="Agent name (cryter/forecaster/archivist)")
    parser.add_argument("--port", type=int, required=True, help="TCP port for this node")
    parser.add_argument("--dash-port", type=int, required=True, help="P2P port of dashboard")
    args = parser.parse_args()

    name = args.name
    dash_peer = f"did:p2p:dashboard@127.0.0.1:{args.dash_port}"

    # Загружаем ключи из Agent Registry
    print(f"[{name}] Loading keys from Agent Registry...")
    keys = load_agent_keys(name)
    from nostr_protocol import Keys as NostrKeys
    nostr_keys = NostrKeys.parse(keys["nsec"])
    hex_pub = keys["hex_pub"] or nostr_keys.public_key().to_hex()
    print(f"[{name}] Keys loaded: npub={keys['npub'][:20]}... hex_pub={hex_pub[:16]}...")
    _nsec_str = keys["nsec"]

    # Создаём транспорт — подключаемся к dashboard
    t = P2PTransport(
        node_id=f"{name}-agent",
        bootstrap_peers=[dash_peer],
    )
    peer_id = await t.start(host="127.0.0.1", port=args.port)
    print(f"[{name}] Started: peer_id={peer_id}, tcp_port={t._tcp_port}")

    # Callback на входящие сообщения
    def on_msg(data):
        try:
            msg = json.loads(data) if isinstance(data, bytes) else data
            from_who = msg.get("from", "?")
            topic = msg.get("topic", "?")
            payload = msg.get("payload", {})
            print(f"[{name}] << {from_who} / {topic}: {str(payload)[:100]}")
        except Exception as e:
            print(f"[{name}] << (raw) {str(data)[:100]}")

    # Подписки
    await t.subscribe("agent:echo", on_msg)
    await t.subscribe("agent:all", on_msg)
    await t.subscribe(f"agent:{name}", on_msg)

    # Helper: подписать и опубликовать
    async def sign_and_publish(topic: str, content: dict, kind: int = 39000):
        event = sign_nostr_event(_nsec_str, content, kind)
        await t.publish(topic, json.dumps(event).encode())
        return event["id"]

    # DHT регистрация — публикуем агента в DHT с Nostr подписью
    dht_value = {
        "agent_id": name,
        "peer_id": peer_id,
        "npub": keys["npub"],
        "hex_pub": hex_pub,
        "capabilities": ["echo", "ping", name, "nostr"],
        "uptime": 0,
        "port": args.port,
    }
    dht_event = sign_nostr_event(_nsec_str, {
        "op": "put",
        "key": f"agent:{name}",
        "value": dht_value,
        "ttl": 3600,
    }, kind=39001)  # kind:39001 = DHT registration

    await t.publish("_dht", json.dumps({
        "topic": "_dht",
        "payload": dht_event,
    }).encode())
    print(f"[{name}] Published to DHT (signed): agent:{name} → {peer_id}")

    # Приветствие в mesh (подписанное)
    hello_content = {
        "type": "hello",
        "from": name,
        "peer_id": peer_id,
        "ts": time.time(),
        "capabilities": ["echo", "ping", name, "nostr"],
    }
    hello_id = await sign_and_publish("agent:echo", hello_content)
    print(f"[{name}] Published hello (signed) id={hello_id[:16]}...")

    # DHT republish loop (каждые 300 сек)
    async def dht_republish():
        while True:
            await asyncio.sleep(300)
            dht_value["uptime"] = round(time.time() - _agent_start, 1)
            dht_event = sign_nostr_event(_nsec_str, {
                "op": "put",
                "key": f"agent:{name}",
                "value": dht_value,
                "ttl": 3600,
            }, kind=39001)
            await t.publish("_dht", json.dumps({
                "topic": "_dht",
                "payload": dht_event,
            }).encode())
            print(f"[{name}] DHT republished (signed)")

    _agent_start = time.time()
    asyncio.create_task(dht_republish())

    # Цикл — heartbeat каждые 30 сек + тематический пост
    counter = 0
    while True:
        await asyncio.sleep(30)
        counter += 1

        # Heartbeat (подписанный)
        hb_content = {
            "type": "heartbeat",
            "from": name,
            "peer_id": peer_id,
            "ts": time.time(),
            "counter": counter,
            "uptime": round(time.time() - _agent_start, 1),
        }
        await sign_and_publish("agent:echo", hb_content)

        # Тематический пост каждые 2 минуты (подписанный)
        if counter % 4 == 0:
            topic_data = {
                "cryter": {"type": "market_update", "coin": "BTC", "sentiment": 0.72,
                           "source": "nostr_signed"},
                "forecaster": {"type": "prediction", "pair": "BTC/USD", "target": 120000,
                               "source": "nostr_signed"},
                "archivist": {"type": "snin_event", "event_kind": 39001, "count": 42,
                              "source": "nostr_signed"},
            }.get(name, {"msg": f"ping from {name}", "source": "nostr_signed"})

            topic_content = {
                "type": topic_data["type"],
                "from": name,
                "ts": time.time(),
                "data": topic_data,
            }
            post_id = await sign_and_publish(f"agent:{name}", topic_content)
            print(f"[{name}] Published {topic_data['type']} (signed) id={post_id[:16]}...")

        peers = list(t._tcp_connections.keys())
        print(f"[{name}] Heartbeat #{counter}. Peers: {len(peers)}")


if __name__ == "__main__":
    asyncio.run(main())
