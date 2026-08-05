#!/usr/bin/env python3
"""
CI Peer for 55-Peer Scale Test — WebSocket version.
Uses `websockets` library (pip install websockets).
Connects to relay, publishes, counts received, sends results.
"""
import asyncio
import json
import os
import sys
import time

import websockets

RELAY_URL = os.environ.get("RELAY_URL", "wss://p2p-dash.v2.site/relay")
PEER_ID = os.environ.get("PEER_ID", f"ci-peer-{os.environ.get('GITHUB_RUN_ID', 'local')}-{os.environ.get('GITHUB_JOB', '0')}")
MSG_COUNT = int(os.environ.get("MSG_COUNT", "10"))
TIMEOUT = int(os.environ.get("TIMEOUT", "120"))

received: list[dict] = []
sent_count = 0


async def run():
    global sent_count
    print(f"[{PEER_ID}] Connecting to {RELAY_URL}...", flush=True)

    try:
        ws = await asyncio.wait_for(
            websockets.connect(RELAY_URL, max_size=2**20), timeout=20
        )
    except Exception as e:
        print(f"[{PEER_ID}] CONNECT FAILED: {e}", flush=True)
        print(f"RESULT:{json.dumps({'status': 'connect_failed', 'error': str(e)})}", flush=True)
        sys.exit(1)

    # Hello
    hello = {"type": "hello", "peer_id": PEER_ID, "topics": ["_all"]}
    await ws.send(json.dumps(hello))

    # Welcome
    try:
        welcome_raw = await asyncio.wait_for(ws.recv(), timeout=15)
        welcome = json.loads(welcome_raw)
        print(f"[{PEER_ID}] Connected! {welcome['peer_count']} peers online", flush=True)
    except Exception as e:
        print(f"[{PEER_ID}] HANDSHAKE FAILED: {e}", flush=True)
        print(f"RESULT:{json.dumps({'status': 'handshake_failed', 'error': str(e)})}", flush=True)
        sys.exit(1)

    # Background reader
    async def reader_task():
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=120)
                msg = json.loads(raw)
                if msg.get("type") == "message":
                    received.append(msg)
        except Exception:
            pass

    reader_ft = asyncio.create_task(reader_task())

    # Wait for peers to join
    await asyncio.sleep(5)

    # Request current stats
    await ws.send(json.dumps({"type": "stats_request"}))
    try:
        stats_raw = await asyncio.wait_for(ws.recv(), timeout=5)
        stats = json.loads(stats_raw)
        peer_count = stats.get("peer_count", 0)
        print(f"[{PEER_ID}] Stats: {peer_count} peers", flush=True)
    except Exception:
        peer_count = welcome.get("peer_count", 0)

    # Publish messages
    t_start = time.time()
    for i in range(MSG_COUNT):
        msg = {
            "type": "publish",
            "topic": "_all",
            "msg_id": f"{PEER_ID}-{i}",
            "data": f"Scale test msg #{i} from {PEER_ID}",
        }
        await ws.send(json.dumps(msg))
        sent_count += 1
        await asyncio.sleep(0.1)
    t_sent = time.time()
    print(f"[{PEER_ID}] Sent {MSG_COUNT} messages in {t_sent - t_start:.1f}s", flush=True)

    # Wait for incoming messages
    remaining = TIMEOUT - (t_sent - t_start) - 10
    if remaining > 0:
        await asyncio.sleep(remaining)

    # Cancel reader
    reader_ft.cancel()
    try:
        await reader_ft
    except asyncio.CancelledError:
        pass

    # Final stats
    await ws.send(json.dumps({"type": "stats_request"}))
    try:
        stats_raw = await asyncio.wait_for(ws.recv(), timeout=5)
        final_stats = json.loads(stats_raw)
    except Exception:
        final_stats = {"error": "stats_timeout"}

    my_received = len(received)
    expected = (peer_count - 1) * MSG_COUNT if peer_count > 1 else 0
    delivery_pct = (my_received / expected * 100) if expected > 0 else 100.0
    elapsed = time.time() - t_start

    result = {
        "status": "ok",
        "peer_id": PEER_ID,
        "peer_count": peer_count,
        "sent": sent_count,
        "received": my_received,
        "expected": expected,
        "delivery_pct": round(delivery_pct, 1),
        "elapsed_sec": round(elapsed, 1),
        "unique_senders": len(set(m["from"] for m in received)),
        "relay_stats": final_stats,
    }
    print(f"RESULT:{json.dumps(result)}", flush=True)

    await ws.close()


if __name__ == "__main__":
    asyncio.run(run())
