"""Preflight (make sure node ports are free, offering to kill what holds them) and
stop (end a running experiment). Starting/holding/tearing down the cluster is
run_cluster.py's job; stop just signals it so its own cleanup() runs.

Every command runs through the Conn, so these never care about local vs jump."""
import sys
import time

from config import pi_target


def port_busy(conn, servers, host, port):
    inner = f"ssh {pi_target(servers, host)} 'fuser {port}/tcp'"
    return conn.run(inner, capture_output=True, text=True).returncode == 0


def preflight(conn, servers, endpoints):
    busy = []
    print("Preflight: checking node ports...")
    for addr in endpoints:
        host, port = addr.split(":")
        if port_busy(conn, servers, host, port):
            print(f"    {addr:24} BUSY")
            busy.append((host, port))
        else:
            print(f"    {addr:24} free")

    if not busy:
        print("    all ports free")
        return

    answer = input(f"\n{len(busy)} port(s) busy. Kill occupying processes? [y/N] ").strip().lower()
    if answer != "y":
        sys.exit("aborted: ports in use")

    for host, port in busy:
        conn.run(f"ssh {pi_target(servers, host)} 'fuser -k {port}/tcp'")
        print(f"    killed {host}:{port}")


def stop(conn, run_id, source_endpoints, servers):
    """Kill the benchmark driver, signal run_cluster so its cleanup() frees the
    cluster, then free the source ports. Patterns are scoped by run id, and a
    bracket on one char keeps pkill from matching its own command line."""
    print("Stopping benchmark...")
    conn.run(f"pkill -9 -f 'experiment-runs/{run_id}/benc[h]mark.json'")

    print("Signalling run_cluster to tear the cluster down...")
    conn.run(f"pkill -TERM -f 'experiment-runs/{run_id}/clu[s]ter.json'")
    time.sleep(3)

    print("Freeing source ports...")
    for addr in source_endpoints:
        host, port = addr.split(":")
        conn.run(f"ssh {pi_target(servers, host)} 'fuser -k {port}/tcp'")
        print(f"    freed {host}:{port}")
