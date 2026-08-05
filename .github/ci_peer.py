#!/usr/bin/env python3
"""
CI Peer for 55-Peer Scale Test — v0.9.0 — multi-round polling.
Fixes early-bird problem: waits for ALL peers to join before sending,
then polls multiple rounds to catch all messages.
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
TARGET_PEERS = int(os.environ.get("TARGET_PEERS", "55"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2.0"))

connect_start = 0.0
connect_time = 0.0
sent_count = 0
received_msgs: list[dict] = []
last_seq: int = 0
errors: list[dict] = []
peer_count: int = 0


def _post(path: str, data: dict, timeout: int = 15) -> dict:
    url = f"{RELAY_URL}{path}"
    body = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get(path: str, timeout: int = 15) -> dict:
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

    print(f"[{PEER_ID}] v0.9.0 multi-round mode — {RELAY_URL}", flush=True)

    # Phase 0: Reset relay if first peer
    if PEER_ID.endswith("-1"):
        try:
            _post("/api/relay/reset", {}, timeout=5)
            print(f"[{PEER_ID}] Relay reset for fresh test", flush=True)
        except Exception:
            pass

    # Phase 1: Register
    connect_start = time.time()
    try:
        resp = _post("/api/relay/register", {"peer_id": PEER_ID})
        connect_time = time.time() - connect_start
        peer_count = resp["peer_count"]
        print(f"[{PEER_ID}] Registered in {connect_time:.1f}s. {peer_count} peers online", flush=True)
    except Exception as e:
        errors.append({"phase": "register", "error": str(e)})
        print(f"[{PEER_ID}] REGISTER FAILED: {e}", flush=True)
        _save_metrics("register_failed")
        sys.exit(1)

    # Phase 2: Wait for ALL peers to join (check peer_count every check_interval)
    wait_deadline = time.time() + TIMEOUT * 0.65
    check_interval = 3.0
    MIN_WAIT = 8.0  # even if target reached, wait at least this long for peers to settle
    join_rounds = 0
    last_seen_count = peer_count
    target_reached_at = time.time() if peer_count >= TARGET_PEERS else None

    while time.time() < wait_deadline and peer_count < TARGET_PEERS:
        try:
            resp = _get(f"/api/relay/poll?since={last_seq}&peer_id={PEER_ID}", timeout=8)
            peer_count = resp.get("peer_count", peer_count)
            join_rounds += 1
            if peer_count > last_seen_count:
                print(f"[{PEER_ID}] Peers: {last_seen_count} → {peer_count}/{TARGET_PEERS} (check #{join_rounds})", flush=True)
                last_seen_count = peer_count
                if peer_count >= TARGET_PEERS and target_reached_at is None:
                    target_reached_at = time.time()
        except Exception:
            pass
        time.sleep(check_interval)

    # Even after target reached, enforce minimum settle time
    if target_reached_at is not None:
        settle_remaining = MIN_WAIT - (time.time() - target_reached_at)
        if settle_remaining > 0:
            print(f"[{PEER_ID}] Target reached, settling for {settle_remaining:.0f}s...", flush=True)
            time.sleep(settle_remaining)
    elif peer_count < TARGET_PEERS:
        print(f"[{PEER_ID}] ⚠ Only {peer_count}/{TARGET_PEERS} after {join_rounds} checks — proceeding anyway", flush=True)

    print(f"[{PEER_ID}] Join phase done: {peer_count}/{TARGET_PEERS} peers, {join_rounds} checks ({time.time() - connect_start:.0f}s elapsed)", flush=True)

    # Phase 2.5: Pre-poll — catch early messages
    try:
        resp = _get(f"/api/relay/poll?since=0&peer_id={PEER_ID}", timeout=10)
        msgs = resp.get("messages", [])
        for m in msgs:
            received_msgs.append(m)
        last_seq = resp.get("last_seq", 0)
        peer_count = resp.get("peer_count", peer_count)
        print(f"[{PEER_ID}] Pre-poll: {len(msgs)} early msgs, seq={last_seq}", flush=True)
    except Exception as e:
        errors.append({"phase": "pre-poll", "error": str(e)})

    # Phase 3: Send messages (fast batch)
    t_send_start = time.time()
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
        time.sleep(0.1)  # 10 msg/s

    send_time = time.time() - t_send_start
    print(f"[{PEER_ID}] Sent {sent_count} in {send_time:.1f}s ({sent_count/send_time:.1f} msg/s)", flush=True)

    # Phase 4: Multi-round polling — keep going until no new messages
    poll_deadline = time.time() + TIMEOUT * 0.25
    expected = (peer_count - 1) * MSG_COUNT if peer_count > 1 else 0
    rounds_without_new = 0
    poll_rounds = 0

    print(f"[{PEER_ID}] Polling (expecting ~{expected}, deadline in {poll_deadline - time.time():.0f}s)...", flush=True)

    while time.time() < poll_deadline and rounds_without_new < 3:
        prev_count = len(received_msgs)
        try:
            resp = _get(f"/api/relay/poll?since={last_seq}&peer_id={PEER_ID}", timeout=10)
            msgs = resp.get("messages", [])
            for m in msgs:
                received_msgs.append(m)
            last_seq = resp.get("last_seq", last_seq)
            peer_count = resp.get("peer_count", peer_count)
        except Exception as e:
            errors.append({"phase": "poll", "error": str(e)})

        poll_rounds += 1
        new_msgs = len(received_msgs) - prev_count
        if new_msgs > 0:
            rounds_without_new = 0
            print(f"[{PEER_ID}] Poll #{poll_rounds}: +{new_msgs} msgs, total={len(received_msgs)}/{expected}", flush=True)
        else:
            rounds_without_new += 1
            print(f"[{PEER_ID}] Poll #{poll_rounds}: no new msgs ({rounds_without_new}/3 dry)", flush=True)

        if len(received_msgs) >= expected:
            print(f"[{PEER_ID}] Got expected {expected} msgs, done", flush=True)
            break

        time.sleep(POLL_INTERVAL)

    # Phase 5: Final stats
    unique_senders = len(set(m["from"] for m in received_msgs))
    delivery_pct = (len(received_msgs) / expected * 100) if expected > 0 else 100.0

    result = {
        "status": "ok",
        "peer_id": PEER_ID,
        "peer_count": peer_count,
        "connect_time_s": round(connect_time, 3),
        "sent": sent_count,
        "received": len(received_msgs),
        "expected": expected,
        "delivery_pct": round(delivery_pct, 1),
        "unique_senders": unique_senders,
        "poll_rounds": poll_rounds,
        "errors": len(errors),
    }
    print(f"RESULT:{json.dumps(result, ensure_ascii=False)}", flush=True)

    _save_metrics("ok", result)


if __name__ == "__main__":
    main()
