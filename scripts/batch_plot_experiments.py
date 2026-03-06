import argparse
import os
import re
from pathlib import Path
from plot_batch_transmissions import plot_experiment

def get_latest_iteration(exp_dir: Path) -> Path:
    """Finds the directory with the highest itr_N inside the experiment directory."""
    itrs = list(exp_dir.glob("itr_*"))
    if not itrs:
        return None
    
    def extract_itr_num(itr_path):
        match = re.search(r'itr_(\d+)', itr_path.name)
        return int(match.group(1)) if match else -1

    latest_itr = max(itrs, key=extract_itr_num)
    return latest_itr

def batch_plot(base_dir: str):
    root = Path(base_dir)
    if not root.exists():
        print(f"Base directory {base_dir} does not exist.")
        return

    output_root = root / "plots"
    output_root.mkdir(exist_ok=True)

    # Iterate through all experiment folders
    for exp_folder in root.iterdir():
        if not exp_folder.is_dir() or exp_folder.name == "plots":
            continue
        
        latest_itr_path = get_latest_iteration(exp_folder)
        if not latest_itr_path:
            print(f"Skipping {exp_folder.name}: No iterations found.")
            continue
        
        itr_name = latest_itr_path.name
        exp_name = exp_folder.name
        
        # Define saving path
        save_file = output_root / f"{exp_name}_{itr_name}.png"
        title = f"{exp_name} - {itr_name}"
        
        print(f"Processing {exp_name} ({itr_name})...")
        plot_experiment(str(latest_itr_path), title=title, save_path=str(save_file))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch plot experiment results from a directory.")
    parser.add_argument("--dir", type=str, default="woolmilk/stefan_exp", help="Base directory containing experiments")
    args = parser.parse_args()
    
    batch_plot(args.dir)
