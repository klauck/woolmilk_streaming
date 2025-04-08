# services/entry_node_service.py
import os
import json
import paramiko
from paramiko import SSHClient, AutoAddPolicy
from typing import List, Optional
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from sqlalchemy.orm import Session
from models.entry_node import EntryNode
from schemas.entry_node import EntryNodeCreate

# Timeout for SSH connections
SSH_TIMEOUT = 5

def create_entry_node(db: Session, node_data: EntryNodeCreate) -> EntryNode:
    """
    Create an entry node in the database and attempt to deploy.
    """
    db_node = EntryNode(
        name=node_data.name,
        ssh_host=node_data.ssh_host,
        ssh_port=node_data.ssh_port,
        ssh_user=node_data.ssh_user,
        ssh_password=node_data.ssh_password,
        serving_host=node_data.serving_host,
        serving_port=node_data.serving_port,
        parquet_files=node_data.parquet_files or {},
        env_name=node_data.env_name,  # New field
        status="stopped",
        status_message=""
    )
    db.add(db_node)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Node with the same name already exists.")

    db.refresh(db_node)

    try:
        # Deploy the node
        deploy_entry_node(db_node)
        # Update the status
        db_node.status = "running"
        db_node.status_message = "Deployed successfully."
    except Exception as e:
        db_node.status = "failed"
        db_node.status_message = f"Failed to deploy: {e}"

    db.commit()
    db.refresh(db_node)
    return db_node

def get_entry_node(db: Session, node_id: int) -> Optional[EntryNode]:
    # Return None if not found
    return db.query(EntryNode).filter(EntryNode.id == node_id).first()

def list_entry_nodes(db: Session) -> List[EntryNode]:
    # Return all nodes
    return db.query(EntryNode).all()

def delete_entry_node(db: Session, node: EntryNode) -> None:
    """
    Delete an entry node from the database and attempt to stop it.
    """
    try:
        stop_remote_node(node)
        node.status = "stopped"
        node.status_message = "Stopped and removed from remote."
        db.commit()
    except Exception as e:
        node.status = "failed"
        node.status_message = f"Failed to stop remote: {e}"
        db.commit()

    db.delete(node)
    db.commit()

def deploy_entry_node(node: EntryNode):
    """
    Deploy the node on the remote server.
    """
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(AutoAddPolicy())

    if node.ssh_password:
        ssh.connect(
            hostname=node.ssh_host,
            port=node.ssh_port,
            username=node.ssh_user,
            password=node.ssh_password,
            timeout=SSH_TIMEOUT
        )
    else:
        key_path = os.path.expanduser("~/.ssh/id_rsa")
        ssh.connect(
            hostname=node.ssh_host,
            port=node.ssh_port,
            username=node.ssh_user,
            key_filename=key_path,
            timeout=SSH_TIMEOUT
        )

    # Retrieve the remote home directory
    stdin, stdout, stderr = ssh.exec_command("echo $HOME")
    home_dir = stdout.read().decode().strip()
    print("home_dir")
    print(home_dir)
    remote_dir = os.path.join(home_dir, f"flight_server_{node.name}")

    # Create the directory in the user's home directory
    ssh.exec_command(f"cd ~ && mkdir -p flight_server_{node.name}")

    sftp = ssh.open_sftp()
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    local_node_script_path = os.path.join(current_file_dir, "entry-node-files", "node-server.py")
    remote_node_script_path = os.path.join(remote_dir, "node_server.py")
    sftp.put(local_node_script_path, remote_node_script_path)
    sftp.close()

    # Kill any old process (if exists) running the node script
    kill_cmd = f"pkill -f {remote_node_script_path}"
    ssh.exec_command(kill_cmd)

    # Activate the environment using the provided env_name
    activate_cmd = f"source ~/{node.env_name}/bin/activate"
    parquet_str = json.dumps(node.parquet_files or {})
    remote_log_file = os.path.join(remote_dir, "logs.txt")
    # Build command to activate the environment and start the node server in background
    cmd = (
        f"nohup bash -c \"{activate_cmd} && python {remote_node_script_path} "
        f"--host {node.serving_host} --port {node.serving_port} "
        f"--parquet_files '{parquet_str}' > {remote_log_file} 2>&1\" &"
    )

    print("cmd")
    print(cmd)

    ssh.exec_command(cmd)
    ssh.close()

def stop_remote_node(node: EntryNode):
    """
    Stop the remote node by killing its process.
    """
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(AutoAddPolicy())

    try:
        if node.ssh_password:
            ssh.connect(
                hostname=node.ssh_host,
                port=node.ssh_port,
                username=node.ssh_user,
                password=node.ssh_password,
                timeout=SSH_TIMEOUT
            )
        else:
            key_path = os.path.expanduser("~/.ssh/id_rsa")
            ssh.connect(
                hostname=node.ssh_host,
                port=node.ssh_port,
                username=node.ssh_user,
                key_filename=key_path,
                timeout=SSH_TIMEOUT
            )
    except Exception as e:
        print(f"Could not connect to {node.ssh_host} (timeout/unreachable). Assuming it's already deleted.")
        return

    # Use the same home directory logic to build the path
    remote_dir = os.path.join(f"/Users/{node.ssh_user}", f"flight_server_{node.name}")
    remote_node_script_path = os.path.join(remote_dir, "node_server.py")
    kill_cmd = f"pkill -f {remote_node_script_path}"

    try:
        ssh.exec_command(kill_cmd)
    except Exception as e:
        print(f"Error stopping remote process: {e}")

    ssh.close()
