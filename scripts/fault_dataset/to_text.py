"""Render collected snapshots as text examples: natural-language logs + options + label.

Usage:
    python scripts/fault_dataset/to_text.py data/fault_dataset.jsonl data/text_dataset.jsonl
"""

import json
import sys

OPTIONS = ["do_nothing", "restart_pod", "scale_up", "rollback_deployment", "drain_node"]


def describe(snap):
    lines = []
    for n in snap["nodes"]:
        if not n["ready"]:
            lines.append(f"Node {n['name']} is NotReady: the kubelet stopped reporting to the control plane.")
    for p in snap["pods"]:
        svc = p["service"]
        if p["waiting"]:
            lines.append(f"Pod {p['name']} ({svc}) is waiting with reason {', '.join(p['waiting'])}.")
        elif not p["ready"]:
            lines.append(f"Pod {p['name']} ({svc}) is not Ready (phase {p['phase']}, {p['restarts']} restarts).")
        if not p["ready"] and p["logs"].strip():
            tail = " | ".join(l.strip()[:200] for l in p["logs"].strip().splitlines()[-3:])
            lines.append(f"Last log lines of {p['name']}: {tail}")
    for d in snap["deployments"]:
        if d["desired"] == 0:
            lines.append(f"Deployment {d['name']} has 0 desired replicas; no pods are running for it.")
        elif d["available"] < d["desired"]:
            lines.append(f"Deployment {d['name']} has {d['available']}/{d['desired']} replicas available.")
    seen = set()
    for e in snap["events"]:
        if e["type"] == "Warning" and (e["reason"], e["obj"]) not in seen:
            seen.add((e["reason"], e["obj"]))
            lines.append(f"Warning event on {e['obj']}: {e['reason']} - {e['msg']}")
    for e in snap["node_events"]:
        if e["reason"] == "NodeNotReady":
            lines.append(f"Node event: {e['obj']} {e['reason']}.")
    return "\n".join(lines) or "All pods Ready, all nodes Ready, no warnings."


def render(rec):
    return {
        "logs": describe(rec["snapshot"]),
        "options": OPTIONS,
        "label": rec["action"],
        "fault": rec["fault"],
    }


if __name__ == "__main__":
    with open(sys.argv[2], "w") as out:
        for line in open(sys.argv[1]):
            r = json.loads(line)
            if r["recovered"]:
                out.write(json.dumps(render(r)) + "\n")
