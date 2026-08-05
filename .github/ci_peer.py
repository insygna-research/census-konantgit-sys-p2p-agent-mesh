#!/usr/bin/env python3
"""
CI Peer for 55-Peer Scale Test — HTTP polling version.
v0.8.0 — pure HTTP (no WebSocket), works through v2.site proxy.
Uses stdlib only (urllib). No external deps needed.
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

RELAY_URL = os.environ.get("RELAY_URL", "https://p2p-dash.v2.site").rstrip("/")
PEER_ID = os.environ.get("PEER_ID", f"ci-peer-{os.environ.get('GITHUB_RUN_ID', 'local')}-{os.environ.get('GITHUB_JOB', '0')}")
MSG_COUNT = int(os.environ.get("MSG_COUNT", "10"))
TIMEOUT = int(os.environ.get("TIMEOUT", "120"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2.0"))

connect_start = 0.0
connect_time = 0.0
sent_count = 0
received_msgs: list[dict] = []
last_seq: int = 0
errors: list[dict] = []
peer_count: int = 0


def _post(path: str, data: dict, timeout: int = 15) -> dict:
    """HTTP POST, returns JSON or raises."""
    url = f"{RELAY_URL}{path}"
    body = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get(path: str, timeout: int = 15) -> dict:
    """HTTP GET, returns JSON or raises."""
    url = f"{RELAY_URL}{path}"
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _save_metrics(status: str, result: dict | None = None):
    if result is None:
        result = {}
    metrics = {
        "status": status,
        "peer_id": PEER_ID,
        "connect_time_s": round(connect_time, 3),
        "messages_sent": sent_count,
        "messages_received": len(received_msgs),
        "errors": errors,
        "result": result,
    }
    fname = f"metrics_peer_{PEER_ID}.json".replace("/", "_").replace(":", "_")
    with open(fname, "w") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(f"[{PEER_ID}] Metrics saved to {fname}", flush=True)


def main():
    global connect_time, sent_count, last_seq, peer_count, received_msgs

    print(f"[{PEER_ID}] HTTP relay mode — {RELAY_URL}", flush=True)

    # Step 0: Reset relay if first peer (peer_id ends with "-1")
    if PEER_ID.endswith("-1"):
        try:
            _post("/api/relay/reset", {}, timeout=5)
            print(f"[{PEER_ID}] Relay reset for fresh test", flush=True)
        except Exception:
            pass  # best effort

    # Step 1: Register
    connect_start = time.time()
    try:
        resp = _post("/api/relay/register", {"peer_id": PEER_ID})
        connect_time = time.time() - connect_start
        peer_count = resp["peer_count"]
        print(f"[{PEER_ID}] Registered in {connect_time:.1f}s. {peer_count} peers online", flush=True)
        # Start polling from seq 0 to catch all
        last_seq = 0
    except Exception as e:
        errors.append({"phase": "register", "error": str(e)})
        print(f"[{PEER_ID}] REGISTER FAILED: {e}", flush=True)
        _save_metrics("register_failed")
        sys.exit(1)

    # Step 2: Wait for others to register
    wait_for = min(TIMEOUT * 0.3, 30)
    print(f"[{PEER_ID}] Waiting {wait_for:.0f}s for peers...", flush=True)
    time.sleep(wait_for)

    # Step 2.5: Pre-poll — catch messages from peers that published early
    try:
        resp = _get(f"/api/relay/poll?since=0&peer_id={PEER_ID}", timeout=10)
        msgs = resp.get("messages", [])
        for m in msgs:
            received_msgs.append(m)
        last_seq = resp.get("last_seq", 0)
        peer_count = resp.get("peer_count", peer_count)
        print(f"[{PEER_ID}] Pre-poll: caught {len(msgs)} early messages, seq={last_seq}", flush=True)
    except Exception as e:
        errors.append({"phase": "pre-poll", "error": str(e)})

    # Step 3: Publish messages
    t_start = time.time()
    for i in range(MSG_COUNT):
        msg_id = f"{PEER_ID}-{i}"
        try:
            resp = _post("/api/relay/publish", {
                "peer_id": PEER_ID,
                "msg_id": msg_id,
                "data": f"Scale test msg #{i} from {PEER_ID}",
            }, timeout=10)
            sent_count += 1
            peer_count = resp.get("peer_count", peer_count)
        except Exception as e:
            errors.append({"phase": "publish", "msg_id": msg_id, "error": str(e)})
            print(f"[{PEER_ID}] PUBLISH {i} FAILED: {e}", flush=True)
        time.sleep(0.25)  # 4 msg/s to avoid rate limiting

    publish_time = time.time() - t_start
    if sent_count > 0:
        print(f"[{PEER_ID}] Sent {sent_count} in {publish_time:.1f}s ({sent_count/publish_time:.1f} msg/s)", flush=True)

    # Step 4: Poll for messages from others
    poll_deadline = time.time() + TIMEOUT - publish_time - 10
    expected = (peer_count - 1) * MSG_COUNT if peer_count > 1 else 0
    print(f"[{PEER_ID}] Polling for messages (expecting ~{expected})...", flush=True)

    while time.time() < poll_deadline:
        try:
            resp = _get(f"/api/relay/poll?since={last_seq}&peer_id={PEER_ID}", timeout=10)
            msgs = resp.get("messages", [])
            for m in msgs:
                received_msgs.append(m)
            last_seq = resp.get("last_seq", last_seq)
            peer_count = resp.get("peer_count", peer_count)
        except Exception as e:
            errors.append({"phase": "poll", "error": str(e)})

        # Stop early if we got enough messages
        if len(received_msgs) >= expected:
            print(f"[{PEER_ID}] Got expected {expected} msgs, stopping poll early", flush=True)
            break
        time.sleep(POLL_INTERVAL)

    # Step 5: Final stats
    unique_senders = len(set(m["from"] for m in received_msgs))
    delivery_pct = (len(received_msgs) / expected * 100) if expected > 0 else 100.0
    elapsed = time.time() - t_start

    result = {
        "status": "ok",
        "peer_id": PEER_ID,
        "peer_count": peer_count,
        "connect_time_s": round(connect_time, 3),
        "sent": sent_count,
        "received": len(received_msgs),
        "expected": expected,
        "delivery_pct": round(delivery_pct, 1),
        "elapsed_sec": round(elapsed, 1),
        "unique_senders": unique_senders,
        "errors": len(errors),
    }
    print(f"RESULT:{json.dumps(result, ensure_ascii=False)}", flush=True)

    _save_metrics("ok", result)


if __name__ == "__main__":
    main()
