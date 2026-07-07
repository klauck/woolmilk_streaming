"""Run pairwise iperf3 to measure simultaneous bandwidth across the Raspberry Pi cluster. Developed by Tristan"""

from __future__ import annotations

import argparse
import itertools
import json
import shlex
import subprocess
import time
from pathlib import Path

SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8"]


def run(
    cmd: list[str], timeout: float | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )


def ssh(
    host: str, command: str, timeout: float | None = None
) -> subprocess.CompletedProcess[str]:
    return run([*SSH, "picocluster@" + host, command], timeout)


def ssh_host_ip(host: str) -> str:
    for line in run(["ssh", "-G", host], 5).stdout.splitlines():
        key, _, value = line.partition(" ")
        if key.lower() == "hostname":
            return value
    return host


def load_hosts(path: Path) -> dict[str, str]:
    hosts = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("[") or line.startswith("#"):
            continue
        parts = shlex.split(line)
        attrs = dict(part.split("=", 1) for part in parts[1:] if "=" in part)
        host = attrs.get("ansible_host", parts[0])
        hosts[host] = ssh_host_ip(host)
    return hosts


def iperf_summary(output: str) -> tuple[float, float]:
    data = json.loads(output[output.find("{") :])
    print(data)
    if "error" in data:
        raise RuntimeError(data["error"])
    end = (
        data["end"].get("sum_received")
        or data["end"].get("sum_sent")
        or data["end"].get("sum")
    )
    bits_per_second = end["bits_per_second"]
    gbps = bits_per_second / 1_000_000_000
    return gbps * 125.0, gbps


def server(host: str, port: int, duration: float) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            *SSH,
            "picocluster@" + host,
            f"timeout {duration + 15:.1f}s iperf3 -s -1 -p {port} -J",
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def one_flow(
    hosts: dict[str, str], sender: str, receiver: str, port: int, duration: float
) -> dict:
    proc = server(receiver, port, duration)
    time.sleep(0.5)
    client = ssh(
        sender, f"iperf3 -c {hosts[receiver]} -p {port} -t {duration} -J", duration + 20
    )
    server_output, _ = proc.communicate(timeout=duration + 20)
    try:
        mb_s, gbps = iperf_summary(server_output)
    except (json.JSONDecodeError, RuntimeError):
        mb_s, gbps = iperf_summary(client.stdout)
    return {
        "sender": sender,
        "receiver": receiver,
        "mb_s": mb_s,
        "gbps": gbps,
        "ok": client.returncode == 0 and proc.returncode == 0,
    }


def simultaneous(
    hosts: dict[str, str], flows: list[tuple[str, str]], port: int, duration: float
) -> dict:
    servers = [
        server(receiver, port + i, duration) for i, (_, receiver) in enumerate(flows)
    ]
    time.sleep(1.0)
    clients = [
        subprocess.Popen(
            [
                *SSH,
                "picocluster@" + sender,
                f"iperf3 -c {hosts[receiver]} -p {port + i} -t {duration} -J",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        for i, (sender, receiver) in enumerate(flows)
    ]
    for client in clients:
        client.communicate(timeout=duration + 20)

    results = []
    for (_, receiver), proc in zip(flows, servers):
        output, _ = proc.communicate(timeout=duration + 20)
        mb_s, gbps = iperf_summary(output)
        results.append({"receiver": receiver, "mb_s": mb_s, "gbps": gbps})

    return {
        "flows": [f"{sender}->{receiver}" for sender, receiver in flows],
        "aggregate_mb_s": sum(item["mb_s"] for item in results),
        "aggregate_gbps": sum(item["gbps"] for item in results),
        "receivers": results,
    }


def parse_flow_set(flow_set: str) -> list[tuple[str, str]]:
    flows = []
    for flow in flow_set.split(","):
        sender, receiver = flow.split(":", 1)
        flows.append((sender.strip(), receiver.strip()))
    return flows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inventory", type=Path, default=Path("../../scripts/inventory_example.ini")
    )
    parser.add_argument("--duration", type=float, default=5.0)
    parser.add_argument("--port", type=int, default=5701)
    parser.add_argument("--output", type=Path, default=Path("iperf_results.json"))
    parser.add_argument(
        "--mode", choices=["pairwise", "contention"], default="pairwise"
    )
    parser.add_argument("--flow-set", action="append", default=[])
    args = parser.parse_args()

    hosts = load_hosts(args.inventory)
    for host in hosts:
        if ssh(host, "command -v iperf3", 10).returncode != 0:
            raise SystemExit(f"iperf3 not found on {host}")

    results = []
    if args.mode == "pairwise":
        pairs = itertools.permutations(hosts, 2)
        for i, (sender, receiver) in enumerate(pairs, start=1):
            print(f"[{i}] {sender} -> {receiver}", flush=True)
            result = one_flow(hosts, sender, receiver, args.port + i, args.duration)
            print(
                f"  {result['mb_s']:.2f} MB/s  {result['gbps']:.3f} Gbit/s", flush=True
            )
            results.append(result)
    else:
        if not args.flow_set:
            raise SystemExit("--mode contention requires at least one --flow-set")
        for flow_set in args.flow_set:
            flows = parse_flow_set(flow_set)
            print(", ".join(f"{s}->{r}" for s, r in flows), flush=True)
            result = simultaneous(hosts, flows, args.port, args.duration)
            print(
                f"  {result['aggregate_mb_s']:.2f} MB/s  {result['aggregate_gbps']:.3f} Gbit/s",
                flush=True,
            )
            results.append(result)

    args.output.write_text(json.dumps(results, indent=2))
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
