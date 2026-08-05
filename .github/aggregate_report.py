#!/usr/bin/env python3
"""Aggregate full-mesh test results into report.md."""
import json, os, glob

report = []
report.append("# 20-Peer Full-Mesh Multi-Round Report")
report.append("")
from datetime import datetime, timezone
report.append(f"**Date:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
run_id = os.environ.get("GITHUB_RUN_ID", "local")
report.append(f"**Run ID:** {run_id}")
report.append("")

# Per-peer table
report.append("## Per-Peer Results")
report.append("")
report.append("| Peer | Sent | Recv | Expected | Delivery % | Senders | Connect(s) | Lat min | Lat med | Lat max | Lat p95 |")
report.append("|------|------|------|----------|------------|---------|------------|---------|---------|---------|---------|")

total_sent = 0
total_recv = 0
total_expected = 0
peers_ok = 0
peers_fail = 0

peer_data = []

for f in sorted(glob.glob("results/*.json")):
    try:
        d = json.load(open(f))
    except (json.JSONDecodeError, FileNotFoundError):
        continue

    pid = os.path.basename(f).replace("result-peer-", "").replace(".json", "")
    sent = d.get("total_sent", 0)
    recv = d.get("total_recv", 0)
    exp = d.get("total_expected", 0)
    pct = d.get("overall_delivery_pct", 0)
    senders = d.get("peer_count", 0)
    ct = d.get("connect_time_s", 0)
    lat = d.get("latency_ms", {}) or {}
    status = d.get("status", "?")

    total_sent += sent
    total_recv += recv
    total_expected += exp

    if status == "ok":
        peers_ok += 1
    else:
        peers_fail += 1

    report.append(
        f"| {pid} | {sent} | {recv} | {exp} | {pct}% | {senders} | {ct}s | "
        f"{lat.get('min','-')} | {lat.get('median','-')} | {lat.get('max','-')} | {lat.get('p95','-')} |"
    )

    # Save round data for later
    peer_data.append({
        "pid": pid,
        "rounds": d.get("rounds", []),
    })

report.append("")
report.append("## Summary")
report.append(f"- **Peers OK:** {peers_ok}")
report.append(f"- **Peers Failed:** {peers_fail}")
report.append(f"- **Total Sent:** {total_sent}")
report.append(f"- **Total Received:** {total_recv}")
report.append(f"- **Total Expected:** {total_expected}")

if total_expected > 0:
    overall = round(total_recv / total_expected * 100, 1)
    report.append(f"- **Overall Delivery:** {overall}%")

# Round-by-round breakdown
report.append("")
report.append("## Round-by-Round Breakdown")
report.append("| Peer | R1 Recv | R1 Del% | R1 Lat med | R2 Recv | R2 Del% | R2 Lat med | R3 Recv | R3 Del% | R3 Lat med |")
report.append("|------|---------|---------|------------|---------|---------|------------|---------|---------|------------|")

for pd in peer_data:
    pid = pd["pid"]
    rounds = pd["rounds"]
    r1 = rounds[0] if len(rounds) > 0 else {}
    r2 = rounds[1] if len(rounds) > 1 else {}
    r3 = rounds[2] if len(rounds) > 2 else {}
    r1l = (r1.get("latency_ms") or {}) if isinstance(r1, dict) else {}
    r2l = (r2.get("latency_ms") or {}) if isinstance(r2, dict) else {}
    r3l = (r3.get("latency_ms") or {}) if isinstance(r3, dict) else {}

    report.append(
        f"| {pid} | {r1.get('received','-')} | {r1.get('delivery_pct','-')}% | {r1l.get('median','-')} | "
        f"{r2.get('received','-')} | {r2.get('delivery_pct','-')}% | {r2l.get('median','-')} | "
        f"{r3.get('received','-')} | {r3.get('delivery_pct','-')}% | {r3l.get('median','-')} |"
    )

# Metrics files listing
report.append("")
report.append("## Metrics Files")
report.append("```")
for f in sorted(glob.glob("metrics/metrics_peer_*.json")):
    path = os.path.join("metrics", os.path.basename(f))
    size = os.path.getsize(f)
    report.append(f"{os.path.basename(f)} — {size} bytes")
if not glob.glob("metrics/metrics_peer_*.json"):
    report.append("No metrics artifacts")
report.append("```")

with open("report.md", "w") as f:
    f.write("\n".join(report))

print("\n".join(report))
