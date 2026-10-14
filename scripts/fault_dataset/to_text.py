"""Convert collected snapshots into examples of raw logs + options + label.

Logs are the unmodified `kubectl logs` output of every pod (healthy ones included,
so a retrieval step has to find the relevant lines), followed by the cluster's
events. Pod log lines are only prefixed with the pod name.

Usage:
    python scripts/fault_dataset/to_text.py data/fault_dataset.jsonl data/text_dataset.jsonl
"""

import json
import sys

OPTIONS = ["do_nothing", "restart_pod", "scale_up", "rollback_deployment", "drain_node"]


def raw_logs(snap):
    lines = [f"[{p['name']}] {line}" for p in snap["pods"] for line in p["logs"].strip().splitlines()]
    for e in snap["events"] + snap["node_events"]:
        lines.append(f"[event {e['ts']}] {e['type']} {e['reason']} {e['obj']}: {e['msg']}")
    return "\n".join(lines)


def render(rec):
    return {"logs": raw_logs(rec["snapshot"]), "options": OPTIONS, "label": rec["action"]}


if __name__ == "__main__":
    with open(sys.argv[2], "w") as out:
        for line in open(sys.argv[1]):
            r = json.loads(line)
            if r["recovered"]:
                out.write(json.dumps(render(r)) + "\n")
