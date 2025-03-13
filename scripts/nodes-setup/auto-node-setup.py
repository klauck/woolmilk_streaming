import json
import os
import sys
import paramiko
from paramiko import SSHClient, AutoAddPolicy

def main():
    current_file_dir = os.path.dirname(os.path.abspath(__file__))

    config_file = os.path.join(current_file_dir, "config.json")

    if not os.path.exists(config_file):
        print(f"Could not find {config_file}")
        sys.exit(1)

    with open(config_file, "r") as f:
        master_config = json.load(f)

    default_config = master_config.get("default_config", {})
    all_nodes = master_config.get("nodes", [])

    for node_data in all_nodes:
        node_name = node_data["name"]
        node_data_dir = node_data.get("data_dir", default_config.get("data_dir", None))
        parquet_files = node_data.get("parquet_files", {})

        for table_name, parquet_name in parquet_files.items():
            parquet_files[table_name] = os.path.join(node_data_dir, parquet_name) if node_data_dir else parquet_name

        merged_node_config = {
            "chunk_size": node_data.get("chunk_size", default_config.get("chunk_size", 100000)),
            "serving_host": node_data.get("serving_host", default_config.get("serving_host", "0.0.0.0")),
            "serving_port": node_data.get("serving_port", default_config.get("serving_port", 8815)),
            "parquet_files": parquet_files
        }

        local_node_config_file = f"node_{node_name}_config.json"

        with open(local_node_config_file, "w") as f:
            json.dump(merged_node_config, f)

        deploy_to_node(node_data, default_config, local_node_config_file)

def deploy_to_node(node_data, default_config, local_node_config_file):
    ssh_host = node_data["ssh_host"]
    ssh_port = node_data.get("ssh_port", default_config.get("ssh_port", 22))
    ssh_user = node_data.get("ssh_user", default_config.get("ssh_user", "root"))
    password = node_data.get("ssh_password", default_config.get("ssh_password", None))
    ssh_key  = node_data.get("ssh_key_path", default_config.get("ssh_key_path", "~/.ssh/id_rsa"))

    node_name = node_data["name"]

    script_name = default_config.get("script_name", "node_server_datafusion.py")
    remote_dir = f"/Users/{ssh_user}/flight_server_{node_name}"

    ssh = SSHClient()
    ssh.set_missing_host_key_policy(AutoAddPolicy())

    if password is not None:
        ssh.connect(hostname=ssh_host, port=ssh_port, username=ssh_user, password=password)
    else:
        ssh.connect(hostname=ssh_host, port=ssh_port, username=ssh_user, key_filename=os.path.expanduser(ssh_key))

    sftp = ssh.open_sftp()

    try:
        sftp.mkdir(remote_dir)
    except IOError:
        pass
    
    current_file_dir = os.path.dirname(os.path.abspath(__file__))

    local_node_script_path = os.path.join(current_file_dir, script_name)
    local_config_path = os.path.join(current_file_dir, local_node_config_file)

    remote_node_script_path = os.path.join(remote_dir, script_name)

    remote_config_path = os.path.join(remote_dir, local_node_config_file)
    remote_log_file = os.path.join(remote_dir, "flight_server.log")

    sftp.put(local_node_script_path, remote_node_script_path)
    sftp.put(local_config_path, remote_config_path)
    sftp.close()

    kill_cmd = f"pkill -f {remote_node_script_path}"
    ssh.exec_command(kill_cmd)
    print(f"[{node_name}] Existing flight server process (if any) killed.")

    python_bin = node_data.get("python_interpreter", default_config.get("python_interpreter", "python3"))

    i_stdin, i_stdout, i_stderr = ssh.exec_command(python_bin + " -m pip show pyarrow datafusion")

    i_out = i_stdout.read().decode()
    i_err = i_stderr.read().decode()

    need_install = False

    if "pyarrow" not in i_out or "datafusion" not in i_out:
        need_install = True
    if need_install:
        print(f"Installing pyarrow and datafusion on node {node_name}...")
        install_stdin, install_stdout, install_stderr = ssh.exec_command(python_bin + " -m pip install --user pyarrow datafusion")
        install_stdout.channel.recv_exit_status()
        print(f"Installed pyarrow and datafusion on node {node_name}.")

    cmd = f"nohup {python_bin} {remote_node_script_path} {remote_config_path} > {remote_log_file} 2>&1 &"

    ssh.exec_command(cmd)
    ssh.close()

    print(f"[{node_name}] Deployed to {ssh_host} and started flight server in background.")
    print(f"Logs on remote: {remote_log_file}")

if __name__ == "__main__":
    main()
