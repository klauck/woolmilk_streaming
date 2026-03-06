import matplotlib.pyplot as plt
import numpy as np
import json
import argparse
from pathlib import Path
from typing import List, Dict, Any

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
    except Exception as e:
        print(f"Error reading JSON from {log_file}: {e}")
        return []

def plot_experiment(experiment_dir: str, title: str = None, save_path: str = None):
    exp_path = Path(experiment_dir)
    if not exp_path.exists():
        print(f"Directory {experiment_dir} not found.")
        return

    # node_batches[label] = [(start, end, batch_id), ...]
    node_batches = {}

    for log_file in exp_path.glob("source__*.log"):
        logs = parse_wmlog_file(log_file)
        for entry in logs:
            sid = entry.get("source_node_id", 0)
            tid = entry.get("thread", 0)
            label = f"Source {sid} (T{tid})"
            
            if label not in node_batches:
                node_batches[label] = []
            
            for times in entry.get("send_times", []):
                if len(times) == 3:
                    node_batches[label].append((times[0], times[1], times[2]))
                elif len(times) == 2:
                    node_batches[label].append((times[0], times[1], len(node_batches[label])))

    for json_file in exp_path.glob("processing__*.json"):
        addr = json_file.stem.split("__")[-1]
        port = addr.split("_")[-1] if "_" in addr else addr
        data = parse_json_file(json_file)
        for entry in data:
            sid = entry.get("source_node_id", 0)
            tid = entry.get("thread_id", 0)
            
            label = f"Proc ({port}, S{sid}, T{tid})"
            
            if label not in node_batches:
                node_batches[label] = []
            
            for times in entry.get("forward_times", []):
                if len(times) == 3:
                    node_batches[label].append((times[0], times[1], times[2]))
                elif len(times) == 2:
                    node_batches[label].append((times[0], times[1], len(node_batches[label])))

    for json_file in exp_path.glob("sink__*.json"):
        addr = json_file.stem.split("__")[-1]
        port = addr.split("_")[-1] if "_" in addr else addr
        data = parse_json_file(json_file)
        for entry in data:
            sid = entry.get("source_node_id", 0)
            tid = entry.get("thread_id", 0)
            
            label = f"Sink ({port}, S{sid}, T{tid})"
            
            if label not in node_batches:
                node_batches[label] = []
            
            for times in entry.get("receive_times", []):
                if len(times) == 3:
                    node_batches[label].append((times[0], times[1], times[2]))
                elif len(times) == 2:
                    node_batches[label].append((times[0], times[1], len(node_batches[label])))

    if not node_batches:
        print("No log data found to plot.")
        return

    labels = list(node_batches.keys())
    x = np.arange(len(labels))

    all_starts = []
    for batches in node_batches.values():
        if batches:
            all_starts.append(batches[0][0])
    global_start = min(all_starts) if all_starts else 0
    
    plt.figure(figsize=(12, 6))
    if title:
        plt.title(title)
        
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
    
    max_batches = max((len(b) for b in node_batches.values()), default=0)
    
    legend_added = set()
    for label_idx, label in enumerate(labels):
        batches = node_batches[label]
        for start, end, batch_id in batches:
            # Only add label to legend if total batches <= 15
            label_name = f'batch {batch_id}' if (batch_id not in legend_added and max_batches <= 15) else ""
            if label_name:
                legend_added.add(batch_id)
            plt.barh(label_idx, end - start,
                     left=start - global_start, 
                     color=colors[batch_id % len(colors)], 
                     hatch='/',
                     label=label_name)

    plt.xlabel('processing time (s)')
    plt.yticks(x, labels)
    if max_batches <= 15:
        plt.legend()
    
    plt.grid(axis='x')
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Plot saved to {save_path}")
    else:
        plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot batch processing times from experiment logs.")
    parser.add_argument("--dir", type=str, required=True, help="Directory containing experiment logs")
    parser.add_argument("--title", type=str, help="Title for the plot")
    parser.add_argument("--save-path", type=str, help="Path to save the plot as an image")
    args = parser.parse_args()
    
    plot_experiment(args.dir, title=args.title, save_path=args.save_path)