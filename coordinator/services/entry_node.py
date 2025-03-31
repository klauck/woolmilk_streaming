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
    Create an entry node in the database and attempt to deploy
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
        # Optional: set a status message
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
        # If stopping the remote process fails, set the status to failed
        node.status = "failed"
        node.status_message = f"Failed to stop remote: {e}"
        db.commit()

    # Now remove from DB
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

    sftp = ssh.open_sftp()

    remote_dir = f"/Users/{node.ssh_user}/flight_server_{node.name}"
    try:
        sftp.mkdir(remote_dir)
    except IOError:
        pass

    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    local_node_script_path = os.path.join(current_file_dir, "entry-node-files", "node-server.py")

    remote_node_script_path = os.path.join(remote_dir, "node_server.py")
    sftp.put(local_node_script_path, remote_node_script_path)
    sftp.close()

    # Kill old process (if any) and start a new one
    kill_cmd = f"pkill -f {remote_node_script_path}"
    ssh.exec_command(kill_cmd)

    python_bin = "python3"
    i_stdin, i_stdout, i_stderr = ssh.exec_command(f"{python_bin} -m pip show pyarrow datafusion")
    i_out = i_stdout.read().decode()

    stdin, stdout, stderr = ssh.exec_command("which python3")

    stdin, stdout, stderr = ssh.exec_command(f"which {python_bin}")

    if "pyarrow" not in i_out:
        ssh.exec_command(f"{python_bin} -m pip install pyarrow datafusion")
    if "datafusion" not in i_out:
        ssh.exec_command(f"{python_bin} -m pip install datafusion")

    parquet_str = json.dumps(node.parquet_files or {})
    remote_log_file = os.path.join(remote_dir, "logs.txt")
    cmd = (
        f"nohup {python_bin} {remote_node_script_path} "
        f"--host {node.serving_host} "
        f"--port {node.serving_port} "
        f'--parquet_files \'{parquet_str}\' '
        f">{remote_log_file} 2>&1 &"
    )

    ssh.exec_command(cmd)
    ssh.close()


def stop_remote_node(node: EntryNode):
    """
    Stop the remote node by killing
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
    except (NoValidConnectionsError, SSHException, socket.timeout) as e:
        print(f"Could not connect to {node.ssh_host} (timeout/unreachable). Assuming it's already deleted.")
        return

    remote_dir = f"/Users/{node.ssh_user}/flight_server_{node.name}"
    remote_node_script_path = os.path.join(remote_dir, "node_server.py")

    kill_cmd = f"pkill -f {remote_node_script_path}"

    try:
        ssh.exec_command(kill_cmd)
    except Exception as e:
        print(f"Error stopping remote process: {e}")

    ssh.close()