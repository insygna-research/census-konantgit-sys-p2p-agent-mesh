#!/usr/bin/env python3
"""
CI Peer for 55-Peer Scale Test.
Pure stdlib (asyncio + json) — no pip install needed.
Connects to mesh relay, publishes messages, counts received, sends results.
"""
import asyncio
import json
import os
import sys
import time

RELAY_HOST = os.environ.get("MESH_RELAY_HOST", "mesh-test.v2.site")
RELAY_PORT = int(os.environ.get("MESH_RELAY_PORT", "9766"))
PEER_ID = os.environ.get("PEER_ID", f"ci-peer-{os.environ.get('GITHUB_RUN_ID', 'local')}-{os.environ.get('GITHUB_JOB', '0')}")
MSG_COUNT = int(os.environ.get("MSG_COUNT", "10"))
TIMEOUT = int(os.environ.get("TIMEOUT", "60"))

received: list[dict] = []
sent_count = 0


async def run():
    global sent_count
    print(f"[{PEER_ID}] Connecting to {RELAY_HOST}:{RELAY_PORT}...", flush=True)

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(RELAY_HOST, RELAY_PORT), timeout=15
        )
    except Exception as e:
        print(f"[{PEER_ID}] CONNECT FAILED: {e}", flush=True)
        result = {"status": "connect_failed", "error": str(e)}
        print(f"RESULT:{json.dumps(result)}", flush=True)
        sys.exit(1)

    # Hello
    hello = {"type": "hello", "peer_id": PEER_ID, "topics": ["_all"]}
    writer.write((json.dumps(hello) + "\n").encode())
    await writer.drain()

    # Welcome
    try:
        welcome_line = await asyncio.wait_for(reader.readline(), timeout=10)
        welcome = json.loads(welcome_line.decode().strip())
        print(f"[{PEER_ID}] Connected! {welcome['peer_count']} peers online", flush=True)
    except Exception as e:
        print(f"[{PEER_ID}] HANDSHAKE FAILED: {e}", flush=True)
        result = {"status": "handshake_failed", "error": str(e)}
        print(f"RESULT:{json.dumps(result)}", flush=True)
        sys.exit(1)

    # Background reader
    async def reader_task():
        while True:
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=5)
                if not line:
                    break
                msg = json.loads(line.decode().strip())
                if msg.get("type") == "message":
                    received.append(msg)
            except asyncio.TimeoutError:
                continue
            except Exception:
                break

    reader_ft = asyncio.create_task(reader_task())

    # Wait for all peers to join (5 more seconds)
    await asyncio.sleep(5)

    # Request stats to see peer count
    writer.write((json.dumps({"type": "stats_request"}) + "\n").encode())
    await writer.drain()
    try:
        stats_line = await asyncio.wait_for(reader.readline(), timeout=3)
        stats = json.loads(stats_line.decode().strip())
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
            "data": f"Scale test message #{i} from {PEER_ID}",
        }
        writer.write((json.dumps(msg) + "\n").encode())
        sent_count += 1
        await asyncio.sleep(0.1)  # 10 msg/sec — не спамим
    await writer.drain()
    t_sent = time.time()
    print(f"[{PEER_ID}] Sent {MSG_COUNT} messages in {t_sent - t_start:.1f}s", flush=True)

    # Wait to receive messages from other peers
    remaining = TIMEOUT - (t_sent - t_start) - 10
    if remaining > 0:
        await asyncio.sleep(remaining)

    # Cancel reader
    reader_ft.cancel()
    try:
        await reader_ft
    except asyncio.CancelledError:
        pass

    # Request final stats
    writer.write((json.dumps({"type": "stats_request"}) + "\n").encode())
    await writer.drain()
    try:
        stats_line = await asyncio.wait_for(reader.readline(), timeout=3)
        final_stats = json.loads(stats_line.decode().strip())
    except Exception:
        final_stats = {"error": "stats_timeout"}

    # Compute results
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

    writer.close()


if __name__ == "__main__":
    asyncio.run(run())
