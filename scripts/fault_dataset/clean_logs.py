"""Deterministic log cleaning for fault-dataset snapshots.

Pipeline (pure functions, same code for training data and live input):
  1. strip ANSI colour codes and timestamp fragments
  2. drop DEBUG/TRACE lines
  3. normalise volatile tokens (uuids, ips, pod hashes, numbers) into a template key
  4. drop lines whose template appears in the "normal templates" baseline, i.e.
     lines that also occur when the cluster is healthy
  5. collapse duplicates (by template) into one line with a count
  6. keep events, except routine pod-startup Normal events
  7. replace pods with no remaining lines by a single summary line

The baseline is built from healthy ("none") episodes only; no fault-specific
keywords are hard-coded.

Usage:
    python clean_logs.py build-baseline fault_dataset.jsonl normal_templates.json
    python clean_logs.py clean fault_dataset.jsonl normal_templates.json out.jsonl
"""

import json
import re
import sys
from collections import OrderedDict

OPTIONS = ["do_nothing", "restart_pod", "scale_up", "rollback_deployment", "drain_node"]

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
LEADING_TS = re.compile(r"^\s*\d{4}-\d\d-\d\dT[\d:.]+(?:Z|[+-]\d\d:\d\d)?\s*")
JSON_TS = re.compile(r'"(?:time|ts)":\s*(?:"[^"]*"|[\d.]+),?|"t":\{"\$date":"[^"]*"\},?')
UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
KUBELET_LOG_URL = re.compile(r"/containerLogs/[^?\s\"]+")
POD = re.compile(r"\b([a-z][a-z0-9-]*?)-[a-z0-9]{8,10}-[a-z0-9]{5}\b")
IP = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b")
HEX = re.compile(r"\b0x[0-9a-fA-F]+\b|\b[0-9a-f]{12,}\b")
DIGITS = re.compile(r"\d+")

LEVEL_JSON = re.compile(r'"level":\s*"(\w+)"')
LEVEL_MONGO = re.compile(r'"s":\s*"([DIWEF])"')
LEVEL_CONSOLE = re.compile(r"\b(DBG|INF|WRN|ERR|FTL|PNC)\b")
LEVEL_BRACKET = re.compile(r"\[(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|ERR)\]")
LEVELS = {"dbg": "debug", "debug": "debug", "trace": "debug", "d": "debug",
          "inf": "info", "info": "info", "i": "info",
          "wrn": "warn", "warn": "warn", "warning": "warn", "w": "warn",
          "err": "error", "error": "error", "e": "error",
          "ftl": "error", "pnc": "error", "fatal": "error", "panic": "error", "f": "error"}

# routine pod-startup events carry no information about what went wrong
ROUTINE_EVENTS = {("Normal", r) for r in ("Scheduled", "Created", "Started", "Pulled")}


def strip_noise(line):
    line = ANSI.sub("", line)
    line = JSON_TS.sub("", line)
    return LEADING_TS.sub("", line).strip()


def level_of(line):
    for rx in (LEVEL_JSON, LEVEL_MONGO, LEVEL_CONSOLE, LEVEL_BRACKET):
        m = rx.search(line)
        if m:
            return LEVELS.get(m.group(1).lower())
    return None


def template(text):
    text = UUID.sub("<uuid>", text)
    text = KUBELET_LOG_URL.sub("/containerLogs/<pod>/<container>", text)
    text = POD.sub(r"\1", text)
    text = IP.sub("<ip>", text)
    text = HEX.sub("<hex>", text)
    return DIGITS.sub("#", text)


def pod_lines(p):
    """Log lines of a pod; the collector keeps only the last 4000 chars, so the first line may be cut."""
    lines = p["logs"].splitlines()
    return lines[1:] if len(p["logs"]) >= 3990 else lines


def clean_line(line):
    """Return (display_text, template) for a log line, or None if it is dropped by level."""
    text = strip_noise(line)
    if not text or level_of(text) == "debug":
        return None
    return text, template(text)


def clean_snapshot(snap, baseline):
    """Return the cleaned log text for one snapshot."""
    seen = OrderedDict()  # template -> [display, pod, count, pods]
    quiet_pods = 0
    for p in snap["pods"]:
        kept = 0
        for line in pod_lines(p):
            res = clean_line(line)
            if res is None:
                continue
            text, key = res
            if key in baseline:
                continue
            kept += 1
            if key in seen:
                seen[key][2] += 1
                seen[key][3].add(p["name"])
            else:
                seen[key] = [text, p["name"], 1, {p["name"]}]
        quiet_pods += kept == 0
    out = []
    for text, pod, n, pods in seen.values():
        suffix = "" if n == 1 else f"  (x{n}" + (f", {len(pods)} pods)" if len(pods) > 1 else ")")
        out.append(f"[{pod}] {text}{suffix}")

    ev = OrderedDict()
    for e in snap["events"] + snap["node_events"]:
        if (e["type"], e["reason"]) in ROUTINE_EVENTS:
            continue
        text = f"{e['type']} {e['reason']} {e['obj']}: {e['msg']}"
        key = template(text)
        if key in ev:
            ev[key][1] += 1
        else:
            ev[key] = [text, 1]
    for text, n in ev.values():
        out.append(f"[event] {text}" + (f"  (x{n})" if n > 1 else ""))

    if quiet_pods:
        out.append(f"[{quiet_pods} of {len(snap['pods'])} pods: no log lines beyond normal startup/health output]")
    if not seen and not ev:
        out.append("No warnings, errors or events.")
    return "\n".join(out)


def build_baseline(records):
    keys = set()
    for r in records:
        if r["fault"] != "none":
            continue
        for p in r["snapshot"]["pods"]:
            for line in pod_lines(p):
                res = clean_line(line)
                if res:
                    keys.add(res[1])
    return keys


def raw_lines(snap):
    n = sum(len(p["logs"].strip().splitlines()) for p in snap["pods"])
    return n + len(snap["events"]) + len(snap["node_events"])


if __name__ == "__main__":
    cmd, src = sys.argv[1], sys.argv[2]
    records = [json.loads(l) for l in open(src)]
    if cmd == "build-baseline":
        keys = sorted(build_baseline(records))
        json.dump(keys, open(sys.argv[3], "w"), indent=0)
        print(f"{len(keys)} normal templates from {sum(r['fault'] == 'none' for r in records)} healthy episodes")
    elif cmd == "clean":
        baseline = set(json.load(open(sys.argv[3])))
        with open(sys.argv[4], "w") as out:
            for r in records:
                if not r["recovered"]:
                    continue
                snap = r["snapshot"]
                cleaned = clean_snapshot(snap, baseline)
                out.write(json.dumps({
                    "logs": cleaned, "options": OPTIONS, "label": r["action"],
                    "raw_lines": raw_lines(snap), "clean_lines": len(cleaned.splitlines()),
                }) + "\n")
