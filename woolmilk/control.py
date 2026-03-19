import json
from time import sleep
from typing import List

from pyarrow import flight

from woolmilk.source_node import SourceNodeActions, NodeStatus

DEFAULT_SLEEP_DURATION = 1

def wait_until_status(nodes, target_status: str, timeout: int | None = None):
    """Wait until all nodes reach a specific target status."""
    nodes_to_wait = list(nodes)

    while len(nodes_to_wait) > 0 and (timeout is None or timeout > 0):
        for node in list(nodes_to_wait):
            try:
                client = flight.FlightClient(f"grpc://{node.server_address}")
                result = list(
                    client.do_action(SourceNodeActions.GET_STATUS)
                )

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


def prepare_source_nodes(nodes, timeout: int | None = None):
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


def wait_until_completion(nodes, timeout: int | None = None):
    """Wait until all nodes have finished their task (report IDLE)."""
    wait_until_status(nodes, NodeStatus.IDLE, timeout)
    print("All nodes finished.")
