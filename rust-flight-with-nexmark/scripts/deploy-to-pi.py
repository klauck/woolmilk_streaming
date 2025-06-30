import subprocess
import sys
import os

def run_scp_manual(source, destination):
    try:
        proc = subprocess.run(['scp', '-o', 'StrictHostKeyChecking=no', source, destination],
                             timeout=120)
        return proc.returncode == 0
    except subprocess.TimeoutExpired:
        return False
    except Exception as e:
        return False

def main():
    if len(sys.argv) < 2:
        binary_path = './flight-with-nexmark'
    else:
        binary_path = sys.argv[1]
    
    if not os.path.exists(binary_path):
        print(f"Binary not found: {binary_path}")
        sys.exit(1)
    
    pi_nodes = ['192.168.2.42', '192.168.2.43']
    user = 'picocluster'
    destination_path = '~/usama'
    
    success_count = 0
    
    for pi_node in pi_nodes:
        destination = f"{user}@{pi_node}:{destination_path}"
        print(f"Transferring to {pi_node}...")
        
        success = run_scp_manual(binary_path, destination)
        
        if success:
            print(f"Successfully transferred to {pi_node}")
            success_count += 1
        else:
            print(f"Failed to transfer to {pi_node}")
    
    print(f"Completed: {success_count}/{len(pi_nodes)} successful")
    
    if success_count > 0:
        print("\nNext steps:")
        print(f"Exit server on {pi_nodes[0]}: ./flight-with-nexmark exit --bind-address \"[::]:8816\"")
        print(f"Processor on {pi_nodes[1]}: ./flight-with-nexmark processor --bind-address \"[::]:8815\" --exit-address \"{pi_nodes[0]}:8816\"")
        print(f"Entry client: ./flight-with-nexmark entry --records-per-chunk 100000 --no-records 10000000 --server-address \"{pi_nodes[1]}:8815\" real-time")

if __name__ == "__main__":
    main()