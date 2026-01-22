import argparse
import json
import re
import sys
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def parse_log_file(log_file: Path) -> List[Dict[str, Any]]:
    logs = []
    with open(log_file, "r") as f:
        for line in f:
            if line.startswith("WM_LOG="):
                try:
                    data = json.loads(line.replace("WM_LOG=", "").strip())
                    data.pop("experiment_id", None)
                    data.pop("iteration_id", None)
                    logs.append(data)
                except json.JSONDecodeError:
                    pass
    return logs

def load_logs(experiment_name: str) -> List[Dict[str, Any]]:
    experiment_dir = PROJECT_ROOT / "woolmilk" / "experiments" / experiment_name
    if not experiment_dir.exists():
        print(f"Error: Experiment directory not found: {experiment_dir}")
        sys.exit(1)

    iterations_data = []
    iter_dirs = sorted([d for d in experiment_dir.iterdir() if d.is_dir()], key=lambda x: x.name)
    
    for iter_dir in iter_dirs:
        match = re.search(r"experiment_(\d+)_iter_(\d+)", iter_dir.name)
        if not match:
            continue
            
        iteration_obj = {
            "experimentId": int(match.group(1)),
            "iterationId": int(match.group(2)),
            "source": [],
            "processing": [],
            "sink": []
        }
        
        source_base = iter_dir / "source"
        if source_base.exists():
            for log_file in sorted(source_base.glob("**/*.log")):
                node_id_match = re.search(r"_(\d+)\.log$", log_file.name)
                node_id = str(node_id_match.group(1)) if node_id_match else "unknown"
                
                logs = parse_log_file(log_file)
                for log in logs:
                    log["nodeId"] = node_id
                    iteration_obj["source"].append(log)

        for node_type in ["processing", "sink"]:
            base_dir = iter_dir / node_type
            if base_dir.exists():
                for log_file in sorted(base_dir.glob("*.log")):
                    parts = log_file.stem.split("_")
                    address = f"{parts[1]}:{parts[2]}" if len(parts) >= 3 else "unknown"
                    
                    logs = parse_log_file(log_file)
                    for log in logs:
                        log["address"] = address
                        iteration_obj[node_type].append(log)
        
        start_times = [l.get("start_time") for l in iteration_obj["source"] if "start_time" in l]
        end_times = [l.get("end_time") for l in iteration_obj["sink"] if "end_time" in l]
        total_bytes = sum(l.get("total_bytes", 0) for l in iteration_obj["source"])
        
        iteration_obj["start"] = min(start_times) if start_times else None
        iteration_obj["end"] = max(end_times) if end_times else None
        iteration_obj["total_bytes"] = total_bytes
        
        iterations_data.append(iteration_obj)

    iterations_data.sort(key=lambda x: (x["experimentId"], x["iterationId"]))
    return iterations_data

def main():
    parser = argparse.ArgumentParser(description="Evaluate Woolmilk Logs")
    parser.add_argument("--experiment-name", type=str, required=True, help="Name of the experiment")
    args = parser.parse_args()
    
    data = load_logs(args.experiment_name)
    
    summary_rows = []
    for it in data:
        row = {
            "Experiment ID": it["experimentId"],
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
