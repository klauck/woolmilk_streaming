import argparse
import csv
import glob
import json
import os
import re
from datetime import datetime

TS_PREFIX = re.compile(r"^(\d{8}_\d{6})_")
CODECS = {"raw": "none", "lz4": "lz4", "zstd": "zstd"}
OPT_MODES = {"none", "comp", "enc"}          # compression/encoding markers, not query tags
BUFFERED = {"buffered": "on", "unbuffered": "off"}

COLUMNS = [
    "time", "experiment", "bps", "buffered", "compression",
    "batch", "query", "iteration", "duration", "throughput",
]


def fmt_time(ts):
    return datetime.strptime(ts, "%Y%m%d_%H%M%S").strftime("%Y-%m-%d %H:%M:%S")


def parse_name(name):
    bps = buffered = batch = query = ""
    compression = "none"
    for tok in name.split("_"):
        if tok == "maxbps" or re.fullmatch(r"\d+bps", tok):
            bps = tok
        elif re.fullmatch(r"\d+k", tok):
            batch = tok
        elif tok in CODECS:
            compression = CODECS[tok]
        elif tok in BUFFERED:
            buffered = BUFFERED[tok]
        elif tok in OPT_MODES:
            continue
        else:
            query = tok if not query else f"{query}_{tok}"
    return {"bps": bps, "buffered": buffered, "compression": compression,
            "batch": batch, "query": query}


def read_source_bytes(itr_dir):
    out = {}
    for f in glob.glob(os.path.join(itr_dir, "source__*.log")):
        for line in open(f):
            if line.startswith("WM_LOG="):
                d = json.loads(line.split("WM_LOG=", 1)[1])
                out[(d["source_node_id"], d["thread"])] = d["total_bytes"]
    return out


def read_sink(itr_dir):
    for f in glob.glob(os.path.join(itr_dir, "sink__*.json")):
        d = json.load(open(f))
        for e in (d if isinstance(d, list) else [d]):
            yield e


def iter_rows(results_dir):
    for run in sorted(os.listdir(results_dir)):
        run_dir = os.path.join(results_dir, run)
        if not os.path.isdir(run_dir):
            continue
        m = TS_PREFIX.match(run)
        name = TS_PREFIX.sub("", run)
        p = parse_name(name)
        p["time"] = fmt_time(m.group(1)) if m else ""
        p["experiment"] = name
        for itr in sorted(os.listdir(run_dir)):
            itr_dir = os.path.join(run_dir, itr)
            if not os.path.isdir(itr_dir):
                continue
            try:
                iteration = int(itr.split("_")[1]) + 1
            except (IndexError, ValueError):
                iteration = itr
            src_bytes = read_source_bytes(itr_dir)
            for e in read_sink(itr_dir):
                dur = e.get("duration")
                sb = src_bytes.get((e["source_node_id"], e["thread_id"]))
                row = dict(p)
                row["iteration"] = iteration
                row["duration"] = dur
                row["throughput"] = (
                    round(sb / (dur * 1e6), 2) if sb and dur else e.get("MBps")
                )
                yield row


def main():
    ap = argparse.ArgumentParser(description="Flatten a results/ folder to CSV (params from folder names)")
    ap.add_argument("results_dir", help="the results/ folder holding <timestamp>_<name>/itr_N/ runs")
    ap.add_argument("-o", "--out", help="output CSV (default <results_dir>/results.csv)")
    args = ap.parse_args()
    out = args.out or os.path.join(args.results_dir, "results.csv")
    with open(out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS)
        w.writeheader()
        n = 0
        for row in iter_rows(args.results_dir):
            w.writerow(row)
            n += 1
    print(f"wrote {n} rows -> {out}")


if __name__ == "__main__":
    main()
