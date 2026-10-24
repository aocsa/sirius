#!/usr/bin/env python3
"""Per-rank, per-query pipeline profile from Quent telemetry -> JSON.

usage: extract.py OUT.json DIR...         one telemetry directory per rank, in rank order
       extract.py --identify DIR...       what each directory says about its rank
       extract.py --auto OUT.json DIR...  order the directories itself and print their labels

Each rank writes its own directory under telemetry_data/. Rank 0 is the one whose
RESULT_COLLECTOR pipeline ran tasks; the host memory space's device_id is the rank's NUMA node
(with two GPUs per node, ranks 0-1 share one node and ranks 2-3 the other). Nothing in the
telemetry tells two ranks on the same node apart beyond that.
"""
import collections, glob, json, sys


def events(d, kind):
    for f in glob.glob(f"{d}/{kind}/*.ndjson"):
        for line in open(f):
            e = json.loads(line)
            k = next(iter(e["data"]))
            yield e["id"], e["timestamp"], k, e["data"][k]


def rank_profile(d):
    pid = next(v["process_id"] for _, _, k, v in events(d, "Worker") if k == "Init")
    host = [
        v["label"]
        for _, _, k, v in events(d, "MemorySpace")
        if k == "Declaration" and "HOST" in v["label"]
    ]
    mem = {
        i: v["label"] for i, _, k, v in events(d, "MemorySpace") if k == "Declaration"
    }
    queries = collections.defaultdict(dict)
    for i, ts, k, v in events(d, "Query"):
        queries[i][k] = ts
    qs = sorted(queries.items(), key=lambda kv: kv[1]["Init"])
    plans = {}
    for i, ts, k, v in events(d, "Plan"):
        if k == "Declaration":
            plans[i] = v
    ops = {i: v for i, ts, k, v in events(d, "Operator") if k == "Declaration"}
    ports = {
        i: v["operator_id"]["target"]
        for i, ts, k, v in events(d, "Port")
        if k == "Declaration"
    }
    tasks = collections.defaultdict(dict)
    for i, ts, k, v in events(d, "Task"):
        t = tasks[i]
        if k == "Created":
            t["pipe"] = v["pipeline_uuid"]["target"]
        t.setdefault(k, ts)
        if k == "Finalizing":
            t["Finalizing"] = ts
    batches = collections.defaultdict(dict)
    for i, ts, k, v in events(d, "DataBatch"):
        b = batches[i]
        if k == "Constructed":
            b["pipe"] = v["producer_pipeline_id"]["target"]
            b["t"] = ts
        elif k == "Stationary" and "bytes" not in b:
            b["bytes"] = v["memory"]["data"]["bytes"]
            b["tier"] = mem.get(v["memory"]["target"], "?")
    out = {"pid": pid, "host_space": host, "queries": []}
    for qid, qt in qs:
        t0 = qt["Executing"]
        plan_ids = [p for p, v in plans.items() if v["query_id"]["target"] == qid]
        pipes = {o: v for o, v in ops.items() if v["plan_id"]["target"] in plan_ids}
        edges = []
        for p in plan_ids:
            for e in plans[p].get("edges", []):
                s, r = ports.get(e["source"]["target"]), ports.get(
                    e["target"]["target"]
                )
                if s in pipes and r in pipes:
                    edges.append([pipes[s]["type_name"], pipes[r]["type_name"]])
        stats = {}
        for t in tasks.values():
            p = t.get("pipe")
            if p not in pipes or "Finalizing" not in t:
                continue
            s = stats.setdefault(p, collections.Counter(n=0, first=1e30, last=0))
            s["n"] += 1
            s["first"] = min(s["first"], t["Created"] - t0)
            s["last"] = max(s["last"], t["Finalizing"] - t0)
            for a, b, name in (
                ("Created", "Reserving", "queue"),
                ("Reserving", "Preparing", "reserve"),
                ("Preparing", "Computing", "prepare"),
                ("Computing", "Finalizing", "compute"),
            ):
                if a in t and b in t:
                    s[name] += t[b] - t[a]
            if "Computing" in t:
                s["cstart"] = min(s.get("cstart", 1e30), t["Computing"] - t0)
        out_b = collections.defaultdict(lambda: [0, 0])
        for b in batches.values():
            if b.get("pipe") in pipes and "bytes" in b:
                out_b[b["pipe"]][0] += 1
                out_b[b["pipe"]][1] += b["bytes"]
        prof = []
        for p, v in pipes.items():
            s = stats.get(p)
            prof.append(
                {
                    "id": v["type_name"].replace("Pipeline Id ", "P"),
                    "label": v["label"],
                    "tasks": s["n"] if s else 0,
                    "start": s["first"] / 1e9 if s else None,
                    "end": s["last"] / 1e9 if s else None,
                    "queue_s": s["queue"] / 1e9 if s else 0,
                    "reserve_s": s["reserve"] / 1e9 if s else 0,
                    "prepare_s": s["prepare"] / 1e9 if s else 0,
                    "compute_s": s["compute"] / 1e9 if s else 0,
                    "batches": out_b[p][0],
                    "bytes": out_b[p][1],
                }
            )
        prof.sort(key=lambda x: int(x["id"][1:]))
        out["queries"].append(
            {
                "plan_s": (qt["Executing"] - qt["Planning"]) / 1e9,
                "exec_s": (qt["Finished"] - qt["Executing"]) / 1e9,
                "wall_start": qt["Init"],
                "pipelines": prof,
                "edges": [
                    [a.replace("Pipeline Id ", "P"), b.replace("Pipeline Id ", "P")]
                    for a, b in edges
                ],
            }
        )
    return out


def numa(r):
    return (
        r["host_space"][0].split("device_id=")[1].split(",")[0]
        if r["host_space"]
        else "?"
    )


def collects(r):
    return r["queries"][-1]["pipelines"][-1]["tasks"] > 0


if sys.argv[1] == "--auto":
    # Rank 0 collects the result; the other rank on its NUMA node is rank 1; the rest are the
    # ranks on the other node, whose order the telemetry does not record.
    profs = [rank_profile(d) for d in sys.argv[3:]]
    r0 = next(r for r in profs if collects(r))
    same = [r for r in profs if r is not r0 and numa(r) == numa(r0)]
    rest = [r for r in profs if r is not r0 and r not in same]
    ordered = [r0] + same + rest
    labels = ["rank 0"] + [f"rank {i + 1}" for i in range(len(same))]
    first = len(same) + 1
    if len(rest) == 2:
        labels += [f"rank {first}/{first + 1} a", f"rank {first}/{first + 1} b"]
    else:
        labels += [f"other node {chr(97 + i)}" for i in range(len(rest))]
    json.dump(ordered, open(sys.argv[2], "w"), indent=1)
    print(",".join(labels))
elif sys.argv[1] == "--identify":
    for d in sys.argv[2:]:
        r = rank_profile(d)
        last = r["queries"][-1]["pipelines"][-1]
        print(
            f"{d}: pid {r['pid']}, {', '.join(r['host_space'])}, "
            f"{'collects the result (rank 0)' if last['tasks'] else 'does not collect the result'}"
        )
else:
    json.dump([rank_profile(d) for d in sys.argv[2:]], open(sys.argv[1], "w"), indent=1)
