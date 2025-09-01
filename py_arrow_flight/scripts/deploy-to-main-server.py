#!/usr/bin/env python3
import os
import subprocess
import sys

CONFIG = {
    "server": {
        "host": "duck-2.dima.tu-berlin.de",
        "user": "usama",
        "ssh_key": "~/.ssh/id_ed25519",
    },
    "paths": {"remote_base": "usama/py-arrow-flight", "local_project_root": None},
    "files": {
        "src": [
            "src/flight_server.py",
            "src/flight_processing_node.py",
            "src/flight_client.py",
            "src/data_generator.py",
        ],
        "scripts": ["scripts/deployment.py", "scripts/config.json"],
    },
}


def run_command(cmd):
    """Run shell command and print output"""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    print(result.stdout)
    return True


def deploy_to_main_server():
    """Deploy all source files and scripts to main server"""

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = CONFIG["paths"]["local_project_root"] or os.path.dirname(current_dir)

    host = CONFIG["server"]["host"]
    ssh_key = CONFIG["server"]["ssh_key"]
    remote_base = CONFIG["paths"]["remote_base"]

    print(f"Deploying to {host}...")
    print(f"Project root: {project_root}")
    print(f"Remote path: {remote_base}")

    create_dir_cmd = (
        f'ssh -i {ssh_key} {host} "mkdir -p {remote_base}/src {remote_base}/scripts"'
    )
    if not run_command(create_dir_cmd):
        return False

    all_files = CONFIG["files"]["src"] + CONFIG["files"]["scripts"]

    for file in all_files:
        local_path = os.path.join(project_root, file)
        if os.path.exists(local_path):
            scp_cmd = f'scp -i {ssh_key} "{local_path}" {host}:{remote_base}/{file}'
            if not run_command(scp_cmd):
                return False
        else:
            print(f"Warning: {local_path} not found")

    print("\nDeployment completed successfully!")
    print(f"Files copied to: {host}:{remote_base}/")
    print("\nTo run on remote server:")
    print(f"ssh -i {ssh_key} {host}")
    print(f"cd {remote_base}/scripts")
    print("python3 deployment.py")

    return True


if __name__ == "__main__":
    if not deploy_to_main_server():
        sys.exit(1)
