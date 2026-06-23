import json
from time import sleep
from typing import Dict, Optional

from pyarrow import flight

from woolmilk.runtime_config import RuntimeConfig
from woolmilk.source_node import NodeStatus, SourceNodeActions

DEFAULT_SLEEP_DURATION = 1
SET_CONFIG_ACTION = "SET_CONFIG"


def wait_until_status(nodes, target_status: str, timeout: Optional[int] = None):
    """Wait until all nodes reach a specific target status."""
    nodes_to_wait = list(nodes)

    while len(nodes_to_wait) > 0 and (timeout is None or timeout > 0):
        for node in list(nodes_to_wait):
            try:
                client = flight.FlightClient(f"grpc://{node.server_address}")
                result = list(client.do_action(SourceNodeActions.GET_STATUS))

                if result:
                    status = result[0].body.to_pybytes().decode("utf-8")
                    print(f"[{node.server_address}] Status: {status}")

                    if status == target_status:
                        nodes_to_wait.remove(node)
            except flight.FlightUnavailableError:
                print(f"[{node.server_address}] Node unavailable, retrying...")
            except Exception as e:
                print(f"Failed to check status for {node.server_address}: {e}")

        if len(nodes_to_wait) > 0:
            sleep(DEFAULT_SLEEP_DURATION)
            if timeout is not None:
                timeout -= DEFAULT_SLEEP_DURATION

    if len(nodes_to_wait) > 0:
        raise RuntimeError(f"Timeout reached while waiting for status {target_status}.")


def trigger_action(nodes, action):
    """Trigger an action on all nodes."""
    for node in nodes:
        try:
            client = flight.FlightClient(f"grpc://{node.server_address}")
            client.do_action(action)
        except Exception as e:
            print(f"Failed to trigger action for {node.server_address}: {e}")


def push_config(nodes, node_config: Dict[str, dict]):
    """Push per-node RuntimeConfig via SET_CONFIG do_action.

    node_config: { server_address (str) -> cfg dict (RuntimeConfig fields) }
    Nodes without an entry are skipped (existing runtime_config preserved).
    """
    for node in nodes:
        addr = node.server_address
        cfg_dict = node_config.get(addr)
        if not cfg_dict:
            print(f"[{addr}] no node_config entry, skipping push")
            continue
        cfg = RuntimeConfig.from_dict(cfg_dict)
        body = cfg.to_json()
        try:
            client = flight.FlightClient(f"grpc://{addr}")
            results = list(client.do_action(flight.Action(SET_CONFIG_ACTION, body)))
            ack = results[0].body.to_pybytes().decode("utf-8") if results else ""
            if ack.startswith("ERR"):
                raise RuntimeError(f"SET_CONFIG rejected by {addr}: {ack}")
            print(f"[{addr}] SET_CONFIG ack: {ack}")
        except Exception as e:
            print(f"Failed to push config to {addr}: {e}")
            raise


def prepare_source_nodes(nodes, timeout: Optional[int] = None):
    """Wait for source nodes to be ready and trigger data generation."""
    print("Preparing source nodes....")
    wait_until_status(nodes, NodeStatus.IDLE, timeout)
    trigger_action(nodes, SourceNodeActions.GENERATE_DATA)
    wait_until_status(nodes, NodeStatus.DATA_GENERATED, timeout)
    print("All source nodes ready.")


def start_sending(nodes):
    """Trigger data transmission from source nodes."""
    print("Starting to send data from source nodes...")
    trigger_action(nodes, SourceNodeActions.SEND_DATA)
    print("Done.")


def wait_until_completion(nodes, timeout: Optional[int] = None):
    """Wait until all nodes have finished their task (report IDLE)."""
    wait_until_status(nodes, NodeStatus.IDLE, timeout)
    print("All nodes finished.")
