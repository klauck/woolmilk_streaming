import argparse
import json
import re
import sys
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd

def parse_wmlog_file(log_file: Path) -> List[Dict[str, Any]]:
    logs = []
    with open(log_file, "r") as f:
        for line in f:
            if line.startswith("WM_LOG="):
                try:
                    data = json.loads(line.replace("WM_LOG=", "").strip())
                    logs.append(data)
                except json.JSONDecodeError:
                    pass
    return logs

def parse_json_file(log_file: Path) -> List[Dict[str, Any]]:
    try:
        with open(log_file, "r") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {log_file}: {e}")
        return []

def load_logs(experiment_dir_path: str) -> List[Dict[str, Any]]:
    experiment_dir = Path(experiment_dir_path).resolve()
    if not experiment_dir.exists():
        print(f"Error: Experiment directory not found: {experiment_dir}")
        sys.exit(1)

    iterations_data = []
    
    iter_dirs = sorted([d for d in experiment_dir.iterdir() if d.is_dir() and d.name.startswith("itr_")], key=lambda x: x.name)

    for iter_dir in iter_dirs:
        match = re.search(r"itr_(\d+)", iter_dir.name)
        if not match:
            continue
            
        iteration_id = int(match.group(1))
        
        iteration_obj = {
            "iterationId": iteration_id,
            "source": [],
            "processing": [],
            "sink": []
        }
        
        for log_file in sorted(iter_dir.glob("source__*.log")):
            node_id_match = re.search(r"source__.*_(\d+)\.log$", log_file.name)

            if not node_id_match:
                continue

            node_id = str(node_id_match.group(1))
            
            logs = parse_wmlog_file(log_file)
            for log in logs:
                log["nodeId"] = node_id
                iteration_obj["source"].append(log)

        for log_file in sorted(iter_dir.glob("processing__*.json")):
            parts = log_file.stem.split("__")

            if len(parts) < 2:
                continue
            
            address = parts[1].replace("_", ":")
            
            logs = parse_json_file(log_file)
            for log in logs:
                log["address"] = address
                iteration_obj["processing"].append(log)

        for log_file in sorted(iter_dir.glob("sink__*.json")):
            parts = log_file.stem.split("__")

            if len(parts) < 2:
                continue

            address = parts[1].replace("_", ":")
            
            logs = parse_json_file(log_file)
            for log in logs:
                log["address"] = address
                iteration_obj["sink"].append(log)
        
        start_times = [l.get("start_time") for l in iteration_obj["source"] if "start_time" in l]
        end_times = [l.get("end_time") for l in iteration_obj["sink"] if "end_time" in l]
        total_bytes = sum(l.get("total_bytes", 0) for l in iteration_obj["source"])
        
        iteration_obj["start"] = min(start_times) if start_times else None
        iteration_obj["end"] = max(end_times) if end_times else None
        iteration_obj["total_bytes"] = total_bytes
        
        iterations_data.append(iteration_obj)

    iterations_data.sort(key=lambda x: (x["iterationId"]))
    return iterations_data

def main():
    parser = argparse.ArgumentParser(description="Evaluate Woolmilk Logs")
    parser.add_argument("--experiment-dir", type=str, required=True, help="Path to the experiment directory (e.g. woolmilk/experiments/timestamp_name)")
    args = parser.parse_args()
    
    data = load_logs(args.experiment_dir)
    
    summary_rows = []
    for it in data:
        row = {
            "Iteration ID": it["iterationId"],
            "Start Time": it["start"],
            "End Time": it["end"],
            "Data (Bytes)": it["total_bytes"]
        }
        if it["start"] and it["end"]:
            duration = it["end"] - it["start"]
            row["Duration"] = duration
            if duration > 0:
                row["Gbps"] = (it["total_bytes"] * 8) / duration / 1_000_000_000
            else:
                row["Gbps"] = 0.0
        else:
            row["Duration"] = None
            row["Gbps"] = 0.0
        
        summary_rows.append(row)
        
    df = pd.DataFrame(summary_rows)
    print(df.to_string(index=False))

if __name__ == "__main__":
    main()
