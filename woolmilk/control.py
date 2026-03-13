import json
from time import sleep
from typing import List

from pyarrow import flight

from woolmilk.util import SourceNodeActions, SourceNodeStatus


def prepare_source_nodes(nodes):
    """Wait for source nodes to be ready and trigger data generation."""
    print("Preparing source nodes....")
    nodes_to_be_prepared = list(nodes)

    while len(nodes_to_be_prepared) > 0:
        try:
            for node in list(nodes_to_be_prepared):
                client = flight.FlightClient(f"grpc://{node.server_address}")
                result = list(
                    client.do_action(flight.Action(SourceNodeActions.GET_STATUS, b""))
                )

                if result:
                    status = result[0].body.to_pybytes().decode("utf-8")
                    print(f"[{node.server_address}] Status: {status}")

                    if status == SourceNodeStatus.NOT_STARTED:
                        print(f"[{node.server_address}] Generating data...")
                        client.do_action(
                            flight.Action(SourceNodeActions.GENERATE_DATA, b"")
                        )
                    if status == SourceNodeStatus.DATA_GENERATED:
                        nodes_to_be_prepared.remove(node)

        except flight.FlightUnavailableError as e:
            print(f"[{node.server_address}] Waiting for node to be ready...")
        except Exception as e:
            print(f"Failed to check status for {node.server_address}: {e}")
            pass

        # sleep so that we don't overwhelm the source nodes
        sleep(1)

    print("All source nodes ready.")


def start_sending(nodes):
    """Trigger data transmission from source nodes."""
    print("Starting to send data from source nodes...")
    for node in nodes:

        client = flight.FlightClient(f"grpc://{node.server_address}")
        client.do_action(flight.Action(SourceNodeActions.SEND_DATA, b""))
    print("Done.")


def wait_until_completion(nodes):
    """Wait until all source nodes have finished sending data."""
    nodes_to_wait = list(nodes)
    print("Waiting for source nodes to complete...")

    while len(nodes_to_wait) > 0:
        for node in list(nodes_to_wait):

            try:
                client = flight.FlightClient(f"grpc://{node.server_address}")
                result = list(
                    client.do_action(flight.Action(SourceNodeActions.GET_STATUS, b""))
                )

                if result:
                    status = result[0].body.to_pybytes().decode("utf-8")
                    print(f"[{node.server_address}] Status: {status}")

                    if status == SourceNodeStatus.DONE:
                        nodes_to_wait.remove(node)
            except flight.FlightUnavailableError:
                # Node might be briefly unavailable during state transitions or just starting up
                print(f"[{node.server_address}] Status check: node briefly unavailable, retrying...")
            except Exception as e:
                print(f"Failed to check completion status for {node.server_address}: {e}")

        if len(nodes_to_wait) > 0:
            # sleep so that we don't overwhelm the source nodes
            sleep(1)
    print("All source nodes finished.")
