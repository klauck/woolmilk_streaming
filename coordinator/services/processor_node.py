import os
import socket
import paramiko
from paramiko import SSHClient, AutoAddPolicy
from typing import List, Optional
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from sqlalchemy.orm import Session

from models.processor_node import ProcessorNode
from schemas.processor_node import ProcessorNodeCreate

SSH_TIMEOUT = 5

def create_processor_node_without_deploy(
    db: Session, node_data: ProcessorNodeCreate, status: str = "stopped", status_message: str = ""
) -> ProcessorNode:
    """
    Create a processor node in the database without deploying it.
    """
    db_node = ProcessorNode(
        name=node_data.name,
        ssh_host="",
        ssh_port=0,
        ssh_user="",
        ssh_password="",
        exit_host=node_data.exit_host,
        exit_port=node_data.exit_port,
        entry_endpoints=[e.dict() for e in node_data.entry_endpoints] if node_data.entry_endpoints else [],
        queries=[q.dict() for q in node_data.queries] if node_data.queries else [],
        status=status,
        status_message=status_message,
        env_name=node_data.env_name
    )
    db.add(db_node)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Processor node with the same name already exists.")
    db.refresh(db_node)
    return db_node

def create_processor_node(db: Session, node_data: ProcessorNodeCreate) -> ProcessorNode:
    """
    Create a processor node in the database and attempt to deploy it.
    """
    db_node = ProcessorNode(
        name=node_data.name,
        ssh_host=node_data.ssh_host,
        ssh_port=node_data.ssh_port,
        ssh_user=node_data.ssh_user,
        ssh_password=node_data.ssh_password,
        exit_host=node_data.exit_host,
        exit_port=node_data.exit_port,
        entry_endpoints=node_data.entry_endpoints or [],
        queries=node_data.queries or [],
        status="stopped",
        status_message="",
        env_name=node_data.env_name
    )
    db.add(db_node)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Processor node with the same name already exists.")
    db.refresh(db_node)

    try:
        deploy_processor_node(db_node)
        db_node.status = "running"
        db_node.status_message = "Deployed successfully."
    except Exception as e:
        db_node.status = "failed"
        db_node.status_message = f"Failed to deploy: {e}"
    db.commit()
    db.refresh(db_node)
    return db_node

def get_processor_node(db: Session, node_id: int) -> Optional[ProcessorNode]:
    return db.query(ProcessorNode).filter(ProcessorNode.id == node_id).first()

def list_processor_nodes(db: Session) -> List[ProcessorNode]:
    return db.query(ProcessorNode).all()

def delete_processor_node(db: Session, node: ProcessorNode) -> None:
    """
    Delete a processor node from the database and attempt to stop it.
    """
    try:
        stop_remote_processor_node(node)
        node.status = "stopped"
        node.status_message = "Stopped and removed from remote."
        db.commit()
    except Exception as e:
        node.status = "failed"
        node.status_message = f"Failed to stop remote: {e}"
        db.commit()

    db.delete(node)
    db.commit()

def build_entry_endpoints_str(endpoints: list) -> str:
    parts = []
    for ep in endpoints:
        name = ep.get("name", "defaultName")
        host = ep.get("host", "localhost")
        port = ep.get("port", 8815)
        services_list = ep.get("services", [])
        if services_list:
            services_str = ",".join(services_list)
            entry_str = f"{name}|{host}|{port}|{services_str}"
        else:
            entry_str = f"{name}|{host}|{port}"
        parts.append(entry_str)
    return ";".join(parts)

def build_queries_str(queries: list) -> str:
    parts = []
    for q in queries:
        node_id = q.get("node_id", "defaultNode")
        queries_list = q.get("queries_string", [])
        if queries_list:
            queries_combined = ",".join(queries_list)
            query_str = f"{node_id}|{queries_combined}"
        else:
            query_str = f"{node_id}|"
        parts.append(query_str)
    return ";".join(parts)

def deploy_processor_node(node: ProcessorNode):
    """
    Deploy the processor node on the remote server.
    This function uploads the processor-node script and starts it with the correct arguments.
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

    remote_dir = f"/Users/{node.ssh_user}/flight_processor_{node.name}"
    try:
        sftp.mkdir(remote_dir)
    except IOError:
        pass

    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    local_node_script_path = os.path.join(current_file_dir, "processor-node-files", "processor-node.py")
    remote_node_script_path = os.path.join(remote_dir, "processor-node.py")
    sftp.put(local_node_script_path, remote_node_script_path)
    sftp.close()

    kill_cmd = f"pkill -f {remote_node_script_path}"
    ssh.exec_command(kill_cmd)

    activate_cmd = f"source ~/{node.env_name}/bin/activate"

    python_bin = "python3"
    endpoints_str = build_entry_endpoints_str(node.entry_endpoints or [])
    queries_str = build_queries_str(node.queries or [])
    remote_log_file = os.path.join(remote_dir, "logs.txt")

    # Wrap everything in bash -c so we can activate environment and then run Python
    cmd = (
        f"nohup bash -c '{activate_cmd} && {python_bin} {remote_node_script_path} "
        f"--entry_endpoints \"{endpoints_str}\" "
        f"--queries \"{queries_str}\" "
        f"--exit_host {node.exit_host} "
        f"--exit_port {node.exit_port} "
        f"--node_id {node.name}' "
        f"> {remote_log_file} 2>&1 &"
    )

    print(cmd)

    ssh.exec_command(cmd)
    ssh.close()

def stop_remote_processor_node(node: ProcessorNode):
    """
    Stop the remote processor node by killing its process.
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
        print(f"Could not connect to {node.ssh_host}: {e}. Assuming it's already stopped.")
        return

    remote_dir = f"/Users/{node.ssh_user}/flight_processor_{node.name}"
    remote_node_script_path = os.path.join(remote_dir, "processor-node.py")
    kill_cmd = f"pkill -f {remote_node_script_path}"
    try:
        ssh.exec_command(kill_cmd)
    except Exception as e:
        print(f"Error stopping remote process: {e}")
    ssh.close()
