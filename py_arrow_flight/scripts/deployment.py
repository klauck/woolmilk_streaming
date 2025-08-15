from dataclasses import dataclass, field
from typing import List, Optional, Dict
import json
import subprocess
import sys
import os
import time
import argparse
from datetime import datetime
import threading
import paramiko

@dataclass
class ServerConfig:
    username: str
    password: str
    base_dir: str
    python_env: str

@dataclass
class SinkNode:
    serverAddress: str

@dataclass
class ProcessingNode:
    serverAddress: str
    sinkNode: str
    query: Optional[str] = None

@dataclass
class ClientProcessingNode:
    address: str

@dataclass
class ClientNode:
    processingNodes: List[ClientProcessingNode]
    stream: str
    records_count: int
    tuple_rate: int
    thread_count: int = 1
    deployment_server: str = None

@dataclass
class Config:
    sinkNodes: List[SinkNode]
    processingNodes: List[ProcessingNode]
    clientNodes: List[ClientNode]
    servers: Dict[str, ServerConfig] = field(default_factory=dict)

def parse_config(json_path: str) -> Config:
    with open(json_path, "r") as f:
        data = json.load(f)
    
    # Parse servers config
    servers = {}
    if "config" in data and "servers" in data["config"]:
        for host, server_data in data["config"]["servers"].items():
            servers[host] = ServerConfig(**server_data)
    
    sink_nodes = [SinkNode(**sn) for sn in data.get("sinkNodes", [])]
    processing_nodes = [ProcessingNode(**pn) for pn in data.get("processingNodes", [])]
    
    client_nodes = []
    for cn in data.get("clientNodes", []):
        proc_nodes = [ClientProcessingNode(**pn) for pn in cn.get("processingNodes", [])]
        client_nodes.append(ClientNode(
            processingNodes=proc_nodes,
            stream=cn.get("stream"),
            records_count=cn.get("records_count"),
            tuple_rate=cn.get("tuple_rate"),
            thread_count=cn.get("thread_count", 1),
            deployment_server=cn.get("deployment_server")
        ))
    
    return Config(
        sinkNodes=sink_nodes,
        processingNodes=processing_nodes,
        clientNodes=client_nodes,
        servers=servers
    )

def log_reader(proc, log_file, proc_type, identifier):
    """Read process output and write to log file in real-time"""
    with open(log_file, 'w') as f:
        for line in iter(proc.stdout.readline, ''):
            if line:
                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                log_line = f"[{timestamp}] {line}"
                f.write(log_line)
                f.flush()
                print(f"[{proc_type.upper()}:{identifier}] {line.strip()}")

class DeploymentRunner:
    def __init__(self, config: Config, src_dir: str = "../src", log_dir: str = "logs", mode: str = "local"):
        self.config = config
        self.src_dir = src_dir
        self.log_dir = log_dir
        self.mode = mode
        self.processes = []
        self.log_threads = []
        self.ssh_connections = {}
        
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_prefix = f"{timestamp}_"
    
    def get_server_config(self, host: str) -> Optional[ServerConfig]:
        """Get server config for host"""
        return self.config.servers.get(host)
    
    def get_ssh_connection(self, host: str, server_config: ServerConfig):
        """Get or create SSH connection"""
        if host not in self.ssh_connections:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host, username=server_config.username, password=server_config.password)
            self.ssh_connections[host] = ssh
        return self.ssh_connections[host]
    
    def kill_remote_processes(self, host: str, server_config: ServerConfig, ports: List[str]):
        """Kill processes on remote host using specified ports"""
        ssh = self.get_ssh_connection(host, server_config)
        for port in ports:
            print(f"Killing processes on {host}:{port}")
            ssh.exec_command(f"kill -9 $(lsof -t -i:{port}) 2>/dev/null || true")
    
    def setup_remote_files(self, host: str, server_config: ServerConfig, files: List[str]):
        """Setup files on remote host"""
        ssh = self.get_ssh_connection(host, server_config)
        
        # Create base directory
        ssh.exec_command(f"mkdir -p {server_config.base_dir}")
        
        # Copy files by creating them with content
        for file_name in files:
            local_path = os.path.join(self.src_dir, file_name)
            remote_path = f"{server_config.base_dir}/{file_name}"
            
            with open(local_path, 'r') as f:
                content = f.read()
            
            # Create file on remote host
            ssh.exec_command(f'cat > {remote_path} << "EOF"\n{content}\nEOF')
            ssh.exec_command(f"chmod +x {remote_path}")
            print(f"Created {file_name} on {host}:{remote_path}")
    
    def run_remote_command(self, host: str, server_config: ServerConfig, cmd: List[str], log_file: str, proc_type: str, identifier: str):
        """Run command on remote host and stream logs"""
        ssh = self.get_ssh_connection(host, server_config)
        
        # Run command directly and stream output with error capture
        # Properly quote arguments that contain spaces
        quoted_cmd = []
        for arg in cmd:
            if ' ' in arg:
                quoted_cmd.append(f"'{arg}'")
            else:
                quoted_cmd.append(arg)
        remote_cmd = f"cd {server_config.base_dir} && {' '.join(quoted_cmd)} 2>&1"
        print(f"Running on {host}: {remote_cmd}")
        
        stdin, stdout, stderr = ssh.exec_command(remote_cmd, get_pty=True)
        
        # Create a mock process object for consistency
        class RemoteProcess:
            def __init__(self, stdout, stderr, ssh_conn):
                self.stdout = stdout
                self.stderr = stderr
                self.returncode = None
                self.ssh_conn = ssh_conn
            
            def poll(self):
                if self.stdout.channel.exit_status_ready():
                    self.returncode = self.stdout.channel.recv_exit_status()
                    return self.returncode
                return None
            
            def terminate(self):
                try:
                    self.stdout.channel.close()
                except:
                    pass
            
            def kill(self):
                self.terminate()
            
            def wait(self, timeout=None):
                return self.stdout.channel.recv_exit_status()
        
        remote_proc = RemoteProcess(stdout, stderr, ssh)
        self.processes.append((proc_type, identifier, remote_proc))
        
        # Start log reader thread
        def remote_log_reader():
            with open(log_file, 'w') as f:
                # Read both stdout and stderr
                for line in iter(stdout.readline, ''):
                    if line:
                        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                        log_line = f"[{timestamp}] {line}"
                        f.write(log_line)
                        f.flush()
                        print(f"[{proc_type.upper()}:{identifier}] {line.strip()}")
                
                # Also capture stderr
                for line in iter(stderr.readline, ''):
                    if line:
                        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                        log_line = f"[{timestamp}] ERROR: {line}"
                        f.write(log_line)
                        f.flush()
                        print(f"[{proc_type.upper()}:{identifier}] ERROR: {line.strip()}")
        
        thread = threading.Thread(target=remote_log_reader)
        thread.daemon = True
        thread.start()
        self.log_threads.append(thread)
        
        return stdin, stdout, stderr

    def run_sink_nodes(self):
        """Start all sink nodes with live logging"""
        print("Starting sink nodes...")
        for sink in self.config.sinkNodes:
            log_file = os.path.join(self.log_dir, f"{self.log_prefix}sink_{sink.serverAddress.replace(':', '_')}.log")
            
            host = sink.serverAddress.split(':')[0]
            server_config = self.get_server_config(host)
            
            if self.mode == "local" or server_config is None:
                # Local execution
                cmd = [
                    sys.executable, "-u",
                    os.path.join(self.src_dir, "flight_server.py"),
                    "--server-address", sink.serverAddress
                ]
                print(f"Running locally: {' '.join(cmd)} > {log_file}")
                
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
                self.processes.append(("sink", sink.serverAddress, proc))
                
                thread = threading.Thread(target=log_reader, args=(proc, log_file, "sink", sink.serverAddress))
                thread.daemon = True
                thread.start()
                self.log_threads.append(thread)
            else:
                # Remote execution
                self.setup_remote_files(host, server_config, ["flight_server.py"])
                
                cmd = [
                    server_config.python_env, "-u",
                    "flight_server.py",
                    "--server-address", sink.serverAddress
                ]
                
                self.run_remote_command(host, server_config, cmd, log_file, "sink", sink.serverAddress)
        time.sleep(2)

    def run_processing_nodes(self):
        """Start all processing nodes with live logging"""
        print("Starting processing nodes...")
        for proc_node in self.config.processingNodes:
            log_file = os.path.join(self.log_dir, f"{self.log_prefix}processing_{proc_node.serverAddress.replace(':', '_')}.log")
            
            host = proc_node.serverAddress.split(':')[0]
            server_config = self.get_server_config(host)
            
            if self.mode == "local" or server_config is None:
                # Local execution
                cmd = [
                    sys.executable, "-u",
                    os.path.join(self.src_dir, "flight_processing_node.py"),
                    "--server-address", proc_node.serverAddress,
                    "--exit_node", proc_node.sinkNode
                ]
                if proc_node.query:
                    cmd.extend(["--query", proc_node.query])
                
                print(f"Running locally: {' '.join(cmd)} > {log_file}")
                
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
                self.processes.append(("processing", proc_node.serverAddress, proc))
                
                thread = threading.Thread(target=log_reader, args=(proc, log_file, "processing", proc_node.serverAddress))
                thread.daemon = True
                thread.start()
                self.log_threads.append(thread)
            else:
                # Remote execution
                self.setup_remote_files(host, server_config, ["flight_processing_node.py"])
                
                cmd = [
                    server_config.python_env, "-u",
                    "flight_processing_node.py",
                    "--server-address", proc_node.serverAddress,
                    "--exit_node", proc_node.sinkNode
                ]
                if proc_node.query:
                    cmd.extend(["--query", proc_node.query])
                
                self.run_remote_command(host, server_config, cmd, log_file, "processing", proc_node.serverAddress)
        time.sleep(2)

    def run_client_nodes(self):
        """Start all client nodes with live logging"""
        print("Starting client nodes...")
        for i, client in enumerate(self.config.clientNodes):
            server_addresses = ",".join([pn.address for pn in client.processingNodes])
            log_file = os.path.join(self.log_dir, f"{self.log_prefix}client_{client.stream.replace('.', '_')}_{i}.log")
            
            # deployment_server is required for client nodes
            if not client.deployment_server:
                raise ValueError(f"Client node {i} missing required 'deployment_server' field")
            
            host = client.deployment_server
            server_config = self.get_server_config(host)
            
            if self.mode == "local" or server_config is None:
                # Local execution
                cmd = [
                    sys.executable, "-u",
                    os.path.join(self.src_dir, "flight_client.py"),
                    "--stream", client.stream,
                    "--tuple-rate", str(client.tuple_rate),
                    "--records-count", str(client.records_count),
                    "--processing-servers", server_addresses,
                    "--thread-count", str(client.thread_count)
                ]
                
                print(f"Running locally: {' '.join(cmd)} > {log_file}")
                
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
                self.processes.append(("client", client.stream, proc))
                
                thread = threading.Thread(target=log_reader, args=(proc, log_file, "client", client.stream))
                thread.daemon = True
                thread.start()
                self.log_threads.append(thread)
            else:
                # Remote execution - add delay to avoid too many concurrent SSH connections
                time.sleep(1)
                self.setup_remote_files(host, server_config, ["flight_client.py", "data_generator.py"])
                
                cmd = [
                    server_config.python_env, "-u",
                    "flight_client.py",
                    "--stream", client.stream,
                    "--tuple-rate", str(client.tuple_rate),
                    "--records-count", str(client.records_count),
                    "--processing-servers", server_addresses,
                    "--thread-count", str(client.thread_count)
                ]
                
                self.run_remote_command(host, server_config, cmd, log_file, "client", client.stream)

    def monitor_processes(self):
        """Monitor processes and show live output"""
        print(f"\n{'='*60}")
        print("LIVE OUTPUT FROM ALL PROCESSES")
        print(f"{'='*60}")
        
        try:
            while True:
                all_done = True
                for proc_type, identifier, proc in self.processes:
                    if proc.poll() is None:
                        all_done = False
                    elif proc.returncode != 0:
                        print(f"[{proc_type.upper()}:{identifier}] Process ended with error (exit code: {proc.returncode})")
                
                if all_done:
                    print("All processes completed.")
                    break
                    
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down all processes...")
            self.cleanup()

    def cleanup(self):
        """Terminate all running processes and close SSH connections"""
        for _, _, proc in self.processes:
            try:
                if hasattr(proc, 'terminate'):
                    proc.terminate()
                    if hasattr(proc, 'wait'):
                        proc.wait(timeout=5)
            except:
                try:
                    if hasattr(proc, 'kill'):
                        proc.kill()
                except:
                    pass
        
        # Close SSH connections
        for ssh in self.ssh_connections.values():
            try:
                ssh.close()
            except:
                pass

    def deploy(self):
        """Deploy the entire system"""
        print(f"\n{'='*60}")
        print(f"STARTING WOOLMILK STREAMING DEPLOYMENT ({self.mode.upper()} MODE)")
        print(f"{'='*60}")
        
        try:
            # Kill existing processes on remote hosts
            if self.mode == "deploy":
                all_ports = set()
                hosts_to_clean = set()
                
                for sink in self.config.sinkNodes:
                    host = sink.serverAddress.split(':')[0]
                    port = sink.serverAddress.split(':')[1]
                    all_ports.add(port)
                    hosts_to_clean.add(host)
                
                for proc_node in self.config.processingNodes:
                    host = proc_node.serverAddress.split(':')[0]
                    port = proc_node.serverAddress.split(':')[1]
                    all_ports.add(port)
                    hosts_to_clean.add(host)
                
                for host in hosts_to_clean:
                    server_config = self.get_server_config(host)
                    if server_config:
                        self.kill_remote_processes(host, server_config, list(all_ports))
            
            self.run_sink_nodes()
            self.run_processing_nodes()
            self.run_client_nodes()
            self.monitor_processes()
        except Exception as e:
            print(f"Error during deployment: {e}")
            self.cleanup()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Streaming Deployment")
    parser.add_argument("mode", choices=["local", "deploy"], help="Deployment mode: local or remote deploy")
    parser.add_argument("--config", default="config.json", help="Configuration file path")
    parser.add_argument("--src-dir", default="../src", help="Source directory path")
    parser.add_argument("--log-dir", default="logs", help="Log directory path")
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"Config file {args.config} not found!")
        sys.exit(1)

    config = parse_config(args.config)
    runner = DeploymentRunner(config, args.src_dir, args.log_dir, args.mode)
    runner.deploy()