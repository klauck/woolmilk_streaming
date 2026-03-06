import os

def main():
    port_to_kill = [2201, 2202, 2203, 8000, 8010, 8017, 8018, 8020, 8027, 8210, 8211, 8815, 8816, 8820, 8910, 8917, 8918, 8927, 9017, 9027]

    for port in port_to_kill:
        # Check if any process is using the port and kill it if so
        cmd = f"lsof -t -i :{port}"
        pids = os.popen(cmd).read().strip()
        if pids:
            print(f"Killing processes on port {port}: {pids.replace('\n', ' ')}")
            os.system(f"kill -9 {pids.replace('\n', ' ')}")

if __name__ == "__main__":
    main()
