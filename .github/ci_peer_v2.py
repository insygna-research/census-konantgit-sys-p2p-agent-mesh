#!/usr/bin/env python3
"""
CI Peer v2 — 20-Peer Full-Mesh Multi-Round Test with Latency Tracking.
v1.0.1 — fixed MIN_WAIT, round sync, embedded latency timestamps.
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
MSG_COUNT = int(os.environ.get("MSG_COUNT", "5"))
TARGET_PEERS = int(os.environ.get("TARGET_PEERS", "20"))
ROUNDS = int(os.environ.get("ROUNDS", "3"))
TIMEOUT = int(os.environ.get("TIMEOUT", "240"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1.5"))
MIN_WAIT = float(os.environ.get("MIN_WAIT", "8.0"))        # settle after target reached
ROUND_SETTLE = float(os.environ.get("ROUND_SETTLE", "4.0"))  # settle between rounds

connect_time = 0.0
peer_count: int = 0
errors: list[dict] = []
rounds_data: list[dict] = []
total_sent = 0
total_recv = 0
all_latencies_ms: list[float] = []


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
        "total_sent": total_sent,
        "total_recv": total_recv,
        "errors": errors,
        "rounds": rounds_data,
        "latency_ms": {
            "min": round(min(all_latencies_ms), 1) if all_latencies_ms else None,
            "median": round(sorted(all_latencies_ms)[len(all_latencies_ms)//2], 1) if all_latencies_ms else None,
            "max": round(max(all_latencies_ms), 1) if all_latencies_ms else None,
            "p95": round(sorted(all_latencies_ms)[int(len(all_latencies_ms)*0.95)], 1) if all_latencies_ms else None,
            "count": len(all_latencies_ms),
        },
        "result": result,
    }
    fname = f"metrics_peer_{PEER_ID}.json".replace("/", "_").replace(":", "_")
    with open(fname, "w") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(f"[{PEER_ID}] Metrics saved to {fname}", flush=True)


def parse_sent_at_from_data(data_str: str) -> float | None:
    """Extract sent_at (epoch ms) from data prefix: 'ts:1234567890|...'."""
    if data_str.startswith("ts:"):
        try:
            pipe_idx = data_str.index("|")
            return float(data_str[3:pipe_idx])
        except (ValueError, IndexError):
            pass
    return None


def poll_all(last_seq: int, deadline: float, expected: int, round_label: str = "") -> tuple[list[dict], int, int, list[float]]:
    """Poll until expected messages or deadline. Returns (messages, new_last_seq, peer_count, latencies_ms)."""
    received: list[dict] = []
    latencies: list[float] = []
    dry_rounds = 0
    rounds = 0

    while time.time() < deadline and dry_rounds < 3:
        prev = len(received)
        try:
            resp = _get(f"/api/relay/poll?since={last_seq}&peer_id={PEER_ID}", timeout=10)
            msgs = resp.get("messages", [])
            for m in msgs:
                received.append(m)
                # Parse latency from data field
                sent_at = parse_sent_at_from_data(m.get("data", ""))
                if sent_at is not None:
                    lat_ms = time.time() * 1000 - sent_at
                    latencies.append(lat_ms)
            last_seq = resp.get("last_seq", last_seq)
            pc = resp.get("peer_count", 0)
        except Exception as e:
            errors.append({"phase": "poll", "error": str(e)})

        rounds += 1
        new_msgs = len(received) - prev
        if new_msgs > 0:
            dry_rounds = 0
            if round_label:
                print(f"[{PEER_ID}] {round_label} poll #{rounds}: +{new_msgs} msgs, total={len(received)}/{expected}", flush=True)
        else:
            dry_rounds += 1
            if round_label:
                print(f"[{PEER_ID}] {round_label} poll #{rounds}: no new msgs ({dry_rounds}/3 dry)", flush=True)

        if len(received) >= expected:
            break
        time.sleep(POLL_INTERVAL)

    return received, last_seq, pc, latencies


def main():
    global connect_time, peer_count, total_sent, total_recv, all_latencies_ms

    print(f"[{PEER_ID}] v1.0.1 full-mesh multi-round — {RELAY_URL}", flush=True)
    global_start = time.time()

    # Phase 0: Reset if first peer
    if PEER_ID.endswith("-1"):
        try:
            _post("/api/relay/reset", {}, timeout=5)
            print(f"[{PEER_ID}] Relay reset", flush=True)
        except Exception:
            pass

    # Phase 1: Register
    try:
        resp = _post("/api/relay/register", {"peer_id": PEER_ID})
        connect_time = time.time() - global_start
        peer_count = resp["peer_count"]
        print(f"[{PEER_ID}] Registered in {connect_time:.1f}s. {peer_count} peers online", flush=True)
    except Exception as e:
        errors.append({"phase": "register", "error": str(e)})
        _save_metrics("register_failed")
        sys.exit(1)

    # Phase 2: Wait for all peers (with MIN_WAIT settle)
    wait_deadline = time.time() + TIMEOUT * 0.4
    check_interval = 2.0
    join_rounds = 0
    last_seen = peer_count
    target_reached_at = None
    last_seq = 0

    # If already at target, mark target as reached NOW (not at registration)
    if peer_count >= TARGET_PEERS:
        target_reached_at = time.time()

    while time.time() < wait_deadline and peer_count < TARGET_PEERS:
        try:
            resp = _get(f"/api/relay/poll?since={last_seq}&peer_id={PEER_ID}", timeout=8)
            peer_count = resp.get("peer_count", peer_count)
            join_rounds += 1
            if peer_count > last_seen:
                print(f"[{PEER_ID}] Peers: {last_seen} → {peer_count}/{TARGET_PEERS} (check #{join_rounds})", flush=True)
                last_seen = peer_count
                if peer_count >= TARGET_PEERS and target_reached_at is None:
                    target_reached_at = time.time()
        except Exception:
            pass
        time.sleep(check_interval)

    # Settle even if target was reached immediately
    if target_reached_at is not None:
        settle_left = MIN_WAIT - (time.time() - target_reached_at)
        if settle_left > 0:
            print(f"[{PEER_ID}] Target reached, settling for {settle_left:.0f}s...", flush=True)
            time.sleep(settle_left)
    elif peer_count < TARGET_PEERS:
        print(f"[{PEER_ID}] ⚠ Only {peer_count}/{TARGET_PEERS} after {join_rounds} checks — proceeding anyway", flush=True)

    print(f"[{PEER_ID}] Join done: {peer_count}/{TARGET_PEERS}, {join_rounds} checks ({time.time() - global_start:.0f}s)", flush=True)
    expected_per_round = (peer_count - 1) * MSG_COUNT if peer_count > 1 else 0
    print(f"[{PEER_ID}] Expected per round: {expected_per_round} msgs", flush=True)

    # Phase 3: Multi-round send + poll
    time_left = TIMEOUT - (time.time() - global_start) - 5
    round_budget = time_left / ROUNDS if ROUNDS > 0 else time_left

    for round_num in range(1, ROUNDS + 1):
        round_start = time.time()
        rlabel = f"R{round_num}"
        print(f"\n[{PEER_ID}] === ROUND {round_num}/{ROUNDS} ===", flush=True)

        # Quick re-check: are we still seeing all peers?
        try:
            resp = _get(f"/api/relay/poll?since={last_seq}&peer_id={PEER_ID}", timeout=8)
            peer_count = resp.get("peer_count", peer_count)
        except Exception:
            pass

        # Send messages with embedded sent_at timestamps
        sent_this_round = 0
        for i in range(MSG_COUNT):
            sent_at_ms = int(time.time() * 1000)
            msg_id = f"{PEER_ID}-r{round_num}-{i}"
            # Embed sent_at in data: "ts:1234567890|actual message"
            data_payload = f"ts:{sent_at_ms}|Round {round_num} msg #{i} from {PEER_ID}"
            try:
                resp = _post("/api/relay/publish", {
                    "peer_id": PEER_ID,
                    "msg_id": msg_id,
                    "data": data_payload,
                }, timeout=10)
                sent_this_round += 1
                peer_count = resp.get("peer_count", peer_count)
            except Exception as e:
                errors.append({"phase": "publish", "round": round_num, "msg_id": msg_id, "error": str(e)})
            time.sleep(0.08)

        total_sent += sent_this_round
        print(f"[{PEER_ID}] {rlabel}: sent {sent_this_round} msgs", flush=True)

        # Wait for other peers to finish sending
        settle_time = min(ROUND_SETTLE, round_budget * 0.25)
        print(f"[{PEER_ID}] {rlabel}: waiting {settle_time:.0f}s for others to send...", flush=True)
        time.sleep(settle_time)

        # Poll
        now = time.time()
        poll_deadline = min(now + round_budget * 0.5, now + 45)
        print(f"[{PEER_ID}] {rlabel}: polling (deadline {poll_deadline - now:.0f}s)...", flush=True)

        round_msgs, last_seq, peer_count, round_latencies = poll_all(
            last_seq, poll_deadline, expected_per_round, rlabel
        )
        total_recv += len(round_msgs)
        all_latencies_ms.extend(round_latencies)

        unique_senders = len(set(m.get("from", "?") for m in round_msgs))
        round_elapsed = time.time() - round_start

        rd = {
            "round": round_num,
            "sent": sent_this_round,
            "received": len(round_msgs),
            "expected": expected_per_round,
            "unique_senders": unique_senders,
            "delivery_pct": round(len(round_msgs) / expected_per_round * 100, 1) if expected_per_round > 0 else 100.0,
            "elapsed_s": round(round_elapsed, 1),
            "latency_ms": {
                "min": round(min(round_latencies), 1) if round_latencies else None,
                "median": round(sorted(round_latencies)[len(round_latencies)//2], 1) if round_latencies else None,
                "max": round(max(round_latencies), 1) if round_latencies else None,
                "p95": round(sorted(round_latencies)[int(len(round_latencies)*0.95)], 1) if round_latencies and len(round_latencies) >= 20 else None,
                "count": len(round_latencies),
            } if round_latencies else None,
        }
        rounds_data.append(rd)
        print(f"[{PEER_ID}] {rlabel} result: {len(round_msgs)}/{expected_per_round} msgs, {unique_senders} senders, {rd['delivery_pct']}%", flush=True)
        if round_latencies:
            rl = rd["latency_ms"]
            print(f"[{PEER_ID}] {rlabel} latency: min={rl['min']}ms med={rl['median']}ms max={rl['max']}ms p95={rl['p95']}ms ({rl['count']} samples)", flush=True)

    # Phase 4: Final report — calculate expected from actual observed senders
    # Use max unique senders across rounds for honest expected count
    max_senders = max((rd["unique_senders"] for rd in rounds_data), default=0)
    effective_senders = max(max_senders, peer_count - 1 if peer_count > 1 else 0)
    # Cap at 19 (TARGET_PEERS - 1) — relay may report inflated counts
    effective_senders = min(effective_senders, TARGET_PEERS - 1) if TARGET_PEERS > 1 else effective_senders
    total_expected = effective_senders * MSG_COUNT * ROUNDS if effective_senders > 0 else (expected_per_round * ROUNDS)
    # Also compute relay-based expected for debugging
    relay_expected = expected_per_round * ROUNDS
    result = {
        "status": "ok",
        "peer_id": PEER_ID,
        "peer_count": peer_count,
        "connect_time_s": round(connect_time, 3),
        "total_sent": total_sent,
        "total_recv": total_recv,
        "total_expected": total_expected,
        "overall_delivery_pct": round(total_recv / max(total_expected, 1) * 100, 1),
        "rounds": rounds_data,  # Include round-by-round data in result
        "latency_ms": {
            "min": round(min(all_latencies_ms), 1) if all_latencies_ms else None,
            "median": round(sorted(all_latencies_ms)[len(all_latencies_ms)//2], 1) if all_latencies_ms else None,
            "max": round(max(all_latencies_ms), 1) if all_latencies_ms else None,
            "p95": round(sorted(all_latencies_ms)[int(len(all_latencies_ms)*0.95)], 1) if all_latencies_ms and len(all_latencies_ms) >= 20 else None,
            "count": len(all_latencies_ms),
        },
        "errors": len(errors),
    }
    print(f"\nRESULT:{json.dumps(result, ensure_ascii=False)}", flush=True)
    _save_metrics("ok", result)


if __name__ == "__main__":
    main()
