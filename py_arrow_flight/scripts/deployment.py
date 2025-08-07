from dataclasses import dataclass, field
from typing import List, Optional
import json
import subprocess
import sys
import os
import time
import argparse
from datetime import datetime
import threading

@dataclass
class SinkNode:
    serverAddress: str

@dataclass
class ProcessingNode:
    serverAddress: str
    query: Optional[str] = None
    sinkNodes: List[str] = field(default_factory=list)

@dataclass
class ClientProcessingNode:
    address: str

@dataclass
class ClientNode:
    processingNodes: List[ClientProcessingNode]
    stream: str
    records_count: int
    tuple_rate: int

@dataclass
class Config:
    sinkNodes: List[SinkNode]
    processingNodes: List[ProcessingNode]
    clientNodes: List[ClientNode]

def parse_config(json_path: str) -> Config:
    with open(json_path, "r") as f:
        data = json.load(f)
    sink_nodes = [SinkNode(**sn) for sn in data.get("sinkNodes", [])]
    processing_nodes = [ProcessingNode(**pn) for pn in data.get("processingNodes", [])]
    client_nodes = []
    for cn in data.get("clientNodes", []):
        proc_nodes = [ClientProcessingNode(**pn) for pn in cn.get("processingNodes", [])]
        client_nodes.append(ClientNode(
            processingNodes=proc_nodes,
            stream=cn.get("stream"),
            records_count=cn.get("records_count"),
            tuple_rate=cn.get("tuple_rate")
        ))
    return Config(
        sinkNodes=sink_nodes,
        processingNodes=processing_nodes,
        clientNodes=client_nodes
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
    def __init__(self, config: Config, src_dir: str = "../src", log_dir: str = "logs"):
        self.config = config
        self.src_dir = src_dir
        self.log_dir = log_dir
        self.processes = []
        self.log_threads = []
        
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_prefix = f"{timestamp}_"

    def run_sink_nodes(self):
        """Start all sink nodes with live logging"""
        print("Starting sink nodes...")
        for sink in self.config.sinkNodes:
            log_file = os.path.join(self.log_dir, f"{self.log_prefix}sink_{sink.serverAddress.replace(':', '_')}.log")
            cmd = [
                sys.executable, "-u",  # -u for unbuffered output
                os.path.join(self.src_dir, "flight_server.py"),
                "--server-address", sink.serverAddress
            ]
            print(f"Running: {' '.join(cmd)} > {log_file}")
            
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            self.processes.append(("sink", sink.serverAddress, proc))
            
            thread = threading.Thread(target=log_reader, args=(proc, log_file, "sink", sink.serverAddress))
            thread.daemon = True
            thread.start()
            self.log_threads.append(thread)
        time.sleep(2)

    def run_processing_nodes(self):
        """Start all processing nodes with live logging"""
        print("Starting processing nodes...")
        for proc_node in self.config.processingNodes:
            exit_node = proc_node.sinkNodes[0] if proc_node.sinkNodes else "localhost:8820"
            log_file = os.path.join(self.log_dir, f"{self.log_prefix}processing_{proc_node.serverAddress.replace(':', '_')}.log")
            cmd = [
                sys.executable, "-u",  # -u for unbuffered output
                os.path.join(self.src_dir, "flight_processing_node.py"),
                "--server-address", proc_node.serverAddress,
                "--exit_node", exit_node
            ]
            if proc_node.query:
                cmd.extend(["--query", proc_node.query])
            
            print(f"Running: {' '.join(cmd)} > {log_file}")
            
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            self.processes.append(("processing", proc_node.serverAddress, proc))
            
            thread = threading.Thread(target=log_reader, args=(proc, log_file, "processing", proc_node.serverAddress))
            thread.daemon = True
            thread.start()
            self.log_threads.append(thread)
        time.sleep(2)

    def run_client_nodes(self):
        """Start all client nodes with live logging"""
        print("Starting client nodes...")
        for i, client in enumerate(self.config.clientNodes):
            server_addresses = ",".join([pn.address for pn in client.processingNodes])
            log_file = os.path.join(self.log_dir, f"{self.log_prefix}client_{client.stream.replace('.', '_')}_{i}.log")
            cmd = [
                sys.executable, "-u",  # -u for unbuffered output
                os.path.join(self.src_dir, "flight_client.py"),
                "--stream", client.stream,
                "--tuple-rate", str(client.tuple_rate),
                "--records-count", str(client.records_count),
                "--processing-servers", server_addresses
            ]
            
            print(f"Running: {' '.join(cmd)} > {log_file}")
            
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            self.processes.append(("client", client.stream, proc))
            
            thread = threading.Thread(target=log_reader, args=(proc, log_file, "client", client.stream))
            thread.daemon = True
            thread.start()
            self.log_threads.append(thread)

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
        """Terminate all running processes"""
        for _, _, proc in self.processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                proc.kill()

    def deploy(self):
        """Deploy the entire system"""
        print(f"\n{'='*60}")
        print("STARTING WOOLMILK STREAMING DEPLOYMENT")
        print(f"{'='*60}")
        
        try:
            self.run_sink_nodes()
            self.run_processing_nodes()
            self.run_client_nodes()
            self.monitor_processes()
        except Exception as e:
            print(f"Error during deployment: {e}")
            self.cleanup()

if __name__ == "__main__":
    current_file_path = os.path.abspath(__file__)
    config_dir = os.path.join(os.path.dirname(current_file_path), "config.json")
    src_dir = os.path.join(os.path.dirname(current_file_path), "../src")

    parser = argparse.ArgumentParser(description="WoolMilk Streaming Deployment")
    parser.add_argument("--config", default=config_dir, help="Configuration file path")
    parser.add_argument("--src-dir", default=src_dir, help="Source directory path")
    parser.add_argument("--log-dir", default="logs", help="Log directory path")
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"Config file {args.config} not found!")
        sys.exit(1)

    config = parse_config(args.config)
    runner = DeploymentRunner(config, args.src_dir, args.log_dir)
    runner.deploy()
