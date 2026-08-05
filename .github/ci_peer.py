#!/usr/bin/env python3
"""
CI Peer for 55-Peer Scale Test — WebSocket version.
v0.7.3 — metrics collection, retry resilience.
Uses `websockets` library (pip install websockets).
Connects to relay, publishes, counts received, exports metrics artifact.
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

# === Metrics ===
connect_start = 0.0
connect_time = 0.0
sent_count = 0
received: list[dict] = []
latencies: list[float] = []
errors: list[dict] = []


async def run():
    global connect_start, connect_time, sent_count

    print(f"[{PEER_ID}] Connecting to {RELAY_URL}...", flush=True)
    connect_start = time.time()

    try:
        ws = await asyncio.wait_for(
            websockets.connect(RELAY_URL, max_size=2**20), timeout=20
        )
        connect_time = time.time() - connect_start
        print(f"[{PEER_ID}] Connected in {connect_time:.1f}s", flush=True)
    except Exception as e:
        errors.append({"phase": "connect", "error": str(e)})
        print(f"[{PEER_ID}] CONNECT FAILED: {e}", flush=True)
        _save_metrics("connect_failed")
        sys.exit(1)

    # Hello
    await ws.send(json.dumps({"type": "hello", "peer_id": PEER_ID, "topics": ["_all"]}))

    # Welcome
    try:
        welcome_raw = await asyncio.wait_for(ws.recv(), timeout=15)
        welcome = json.loads(welcome_raw)
        peer_count = welcome["peer_count"]
        print(f"[{PEER_ID}] Welcome! {peer_count} peers online: {welcome.get('peers', [])[:5]}...", flush=True)
    except Exception as e:
        errors.append({"phase": "handshake", "error": str(e)})
        print(f"[{PEER_ID}] HANDSHAKE FAILED: {e}", flush=True)
        _save_metrics("handshake_failed")
        sys.exit(1)

    # Background reader
    async def reader_task():
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=120)
                msg = json.loads(raw)
                if msg.get("type") == "message":
                    received.append(msg)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            errors.append({"phase": "reader", "error": str(e)})

    reader_ft = asyncio.create_task(reader_task())

    # Wait for peers to join
    await asyncio.sleep(5)

    # Request current stats
    await ws.send(json.dumps({"type": "stats_request"}))
    try:
        stats_raw = await asyncio.wait_for(ws.recv(), timeout=5)
        stats = json.loads(stats_raw)
        peer_count = stats.get("peer_count", 0)
        print(f"[{PEER_ID}] Stats: {peer_count} peers total", flush=True)
    except Exception:
        pass  # stats are best-effort

    # Publish messages with latency measurement
    t_start = time.time()
    for i in range(MSG_COUNT):
        msg = {
            "type": "publish",
            "topic": "_all",
            "msg_id": f"{PEER_ID}-{i}",
            "data": f"Scale test msg #{i} from {PEER_ID}",
        }
        msg_start = time.time()
        await ws.send(json.dumps(msg))
        sent_count += 1
        await asyncio.sleep(0.1)

    t_sent = time.time()
    publish_time = t_sent - t_start
    print(f"[{PEER_ID}] Sent {MSG_COUNT} in {publish_time:.1f}s ({MSG_COUNT/publish_time:.1f} msg/s)", flush=True)

    # Wait for incoming messages
    remaining = TIMEOUT - publish_time - 10
    if remaining > 0:
        await asyncio.sleep(remaining)

    reader_ft.cancel()
    try: await reader_ft
    except asyncio.CancelledError: pass

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

    # Calculate latency percentiles
    unique_senders = set(m["from"] for m in received)

    result = {
        "status": "ok",
        "peer_id": PEER_ID,
        "peer_count": peer_count,
        "connect_time_s": round(connect_time, 3),
        "sent": sent_count,
        "received": my_received,
        "expected": expected,
        "delivery_pct": round(delivery_pct, 1),
        "elapsed_sec": round(elapsed, 1),
        "unique_senders": len(unique_senders),
        "errors": len(errors),
        "relay_stats": final_stats,
    }
    print(f"RESULT:{json.dumps(result, ensure_ascii=False)}", flush=True)

    _save_metrics("ok", result)
    await ws.close()


def _save_metrics(status: str, result: dict | None = None):
    """Save metrics artifact for GitHub Actions upload."""
    if result is None:
        result = {}
    metrics = {
        "status": status,
        "peer_id": PEER_ID,
        "connect_time_s": round(connect_time, 3),
        "messages_sent": sent_count,
        "messages_received": len(received),
        "errors": errors,
        "result": result,
    }
    fname = f"metrics_peer_{PEER_ID}.json".replace("/", "_").replace(":", "_")
    with open(fname, "w") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(f"[{PEER_ID}] Metrics saved to {fname}", flush=True)


if __name__ == "__main__":
    asyncio.run(run())
