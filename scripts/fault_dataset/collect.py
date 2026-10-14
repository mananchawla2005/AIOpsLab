"""Collect a fault -> telemetry -> remediation-action dataset on a kind cluster.

AIOpsLab is used only as the infrastructure layer (app deployment + fault
injection). No agent / orchestrator loop is involved.

Each episode: inject a fault, wait for it to propagate, snapshot cluster state
(pods, nodes, deployments, events, pod logs), recover, and append one JSON line
to the output file. The label is the remediation action an operator should take.

Usage (cluster + HotelReservation already running):
    python scripts/fault_dataset/collect.py --episodes 4 --out data/fault_dataset.jsonl
"""

import argparse
import json
import random
import subprocess
import time

from aiopslab.generators.fault.inject_virtual import VirtualizationFaultInjector

NS = "test-hotel-reservation"
SERVICES = ["frontend", "geo", "profile", "rate", "recommendation", "reservation", "search", "user"]
KIND_NET = "kind"
WORKERS = ["kind-worker", "kind-worker2"]


def sh(cmd, timeout=120):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:  # e.g. kubectl logs to a node that lost its network
        return f"[ERROR] command timed out after {timeout}s: {cmd.split(' 2>')[0]}"
    return r.stdout if r.returncode == 0 else r.stdout + r.stderr


def kjson(args):
    try:
        return json.loads(sh(f"kubectl {args} -o json"))
    except json.JSONDecodeError:
        return {"items": []}


def wait_healthy(timeout=240):
    """Wait until every deployment is fully available and all nodes are Ready."""
    end = time.time() + timeout
    while time.time() < end:
        deps = kjson(f"get deploy -n {NS}")["items"]
        nodes = kjson("get nodes")["items"]
        deps_ok = all(
            d["status"].get("availableReplicas", 0) == d["spec"]["replicas"] for d in deps
        )
        nodes_ok = all(
            any(c["type"] == "Ready" and c["status"] == "True" for c in n["status"]["conditions"])
            for n in nodes
        )
        if deps_ok and nodes_ok and deps:
            return True
        time.sleep(5)
    return False


def snapshot(since):
    pods = []
    for p in kjson(f"get pods -n {NS}")["items"]:
        cs = p["status"].get("containerStatuses", [])
        waiting = [c["state"]["waiting"]["reason"] for c in cs if "waiting" in c["state"]]
        name = p["metadata"]["name"]
        ready = bool(cs) and all(c["ready"] for c in cs)
        # raw logs from every pod, healthy ones included: a retrieval step must learn which lines matter
        logs = sh(f"kubectl logs {name} -n {NS} --tail=25 --all-containers 2>&1", 15)
        pods.append({
            "name": name,
            "service": p["metadata"].get("labels", {}).get("io.kompose.service", name),
            "phase": p["status"]["phase"],
            "node": p["spec"].get("nodeName"),
            "ready": ready,
            "restarts": sum(c["restartCount"] for c in cs),
            "waiting": waiting,
            "logs": logs[-4000:],
        })
    nodes = [{
        "name": n["metadata"]["name"],
        "ready": any(c["type"] == "Ready" and c["status"] == "True" for c in n["status"]["conditions"]),
        "unschedulable": bool(n["spec"].get("unschedulable")),
    } for n in kjson("get nodes")["items"]]
    deps = [{
        "name": d["metadata"]["name"],
        "desired": d["spec"]["replicas"],
        "available": d["status"].get("availableReplicas", 0),
    } for d in kjson(f"get deploy -n {NS}")["items"]]
    def recent(e):  # only events after the fault was injected (avoid cross-episode leakage)
        ts = e.get("lastTimestamp") or e.get("eventTime") or e["metadata"]["creationTimestamp"]
        return ts >= since

    events = [{"ts": e.get("lastTimestamp") or e["metadata"]["creationTimestamp"], "type": e.get("type"),
               "reason": e.get("reason"), "obj": e["involvedObject"]["name"], "msg": e.get("message", "")}
              for e in kjson(f"get events -n {NS}")["items"] if recent(e)]
    node_events = [{"ts": e.get("lastTimestamp") or e["metadata"]["creationTimestamp"], "type": e.get("type"),
                    "reason": e.get("reason"), "obj": e["involvedObject"]["name"], "msg": e.get("message", "")}
                   for e in kjson("get events -A --field-selector involvedObject.kind=Node")["items"]
                   if recent(e)]
    return {"pods": pods, "nodes": nodes, "deployments": deps,
            "events": events, "node_events": node_events}


# ---- faults: (inject, recover, label=remediation action) -------------------

def make_faults(inj):
    def pick():
        return random.choice(SERVICES)

    def noop():
        return {}, lambda: None

    def pod_kill():
        s = pick()
        sh(f"kubectl delete pod -n {NS} -l io.kompose.service={s} --wait=false")
        return {"target": s}, lambda: None  # kubernetes self-heals

    def scale_zero():
        s = pick()
        inj.inject_scale_pods_to_zero([s])
        return {"target": s}, lambda: inj.recover_scale_pods_to_zero([s])

    def bad_image():
        s = pick()
        sh(f"kubectl set image deploy/{s} -n {NS} hotel-reserv-{s}=yinfangchen/hotelreservation:does-not-exist")
        return {"target": s}, lambda: sh(f"kubectl rollout undo deploy/{s} -n {NS}")

    def crashloop():
        s = pick()
        sh(f"""kubectl patch deploy {s} -n {NS} -p '{{"spec":{{"template":{{"spec":{{"containers":[{{"name":"hotel-reserv-{s}","command":["/bin/false"]}}]}}}}}}}}'""")
        return {"target": s}, lambda: sh(f"kubectl rollout undo deploy/{s} -n {NS}")

    def node_ip_vanish():
        n = random.choice(WORKERS)
        sh(f"docker network disconnect {KIND_NET} {n}")  # node loses its IP / network
        return {"target": n}, lambda: sh(f"docker network connect {KIND_NET} {n}")

    # name -> (fn, remediation label, settle seconds)
    return {
        "none": (noop, "do_nothing", 10),
        "pod_kill": (pod_kill, "do_nothing", 8),
        "scale_to_zero": (scale_zero, "scale_up", 15),
        "bad_image": (bad_image, "rollback_deployment", 30),
        "crashloop": (crashloop, "rollback_deployment", 45),
        "node_ip_vanish": (node_ip_vanish, "drain_node", 70),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--episodes", type=int, default=3, help="episodes per fault type")
    ap.add_argument("--out", default="data/fault_dataset.jsonl")
    ap.add_argument("--seed", type=int, default=0)
    a = ap.parse_args()
    random.seed(a.seed)

    inj = VirtualizationFaultInjector(NS)
    faults = make_faults(inj)
    plan = [f for f in faults for _ in range(a.episodes)]
    random.shuffle(plan)

    assert wait_healthy(), "cluster not healthy before starting"
    with open(a.out, "a") as out:
        for i, name in enumerate(plan):
            fn, action, settle = faults[name]
            t0 = time.time()
            since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            meta, recover = fn()
            time.sleep(settle)
            snap = snapshot(since)
            recover()
            ok = wait_healthy()
            rec = {"episode": i, "fault": name, "action": action, "meta": meta,
                   "settle_s": settle, "recovered": ok, "snapshot": snap}
            out.write(json.dumps(rec) + "\n")
            out.flush()
            print(f"[{i + 1}/{len(plan)}] {name:15s} -> {action:20s} meta={meta} "
                  f"recovered={ok} ({time.time() - t0:.0f}s)", flush=True)
            if not ok:
                print("cluster failed to recover; stopping")
                break
            time.sleep(5)


if __name__ == "__main__":
    main()
