import argparse
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import flight
from util import SourceNodeActions, SourceNodeStatus


class SourceNode(flight.FlightServerBase):
    def __init__(
        self,
        location,
        processing_nodes,
        overall_tuples,
        tuples_per_batch,
        event_type,
        generator_executable,
        offset,
        step,
        thread_count,
        store_input,
        experiment_id,
        iteration_id,
        source_node_id,
    ):
        super().__init__(location)
        self.location = location
        self.current_status = SourceNodeStatus.NOT_STARTED
        self.processing_nodes = processing_nodes
        self.overall_tuples = overall_tuples
        self.tuples_per_batch = tuples_per_batch
        self.event_type = event_type
        self.generator_executable = generator_executable
        self.offset = offset
        self.step = step
        self.thread_count = thread_count
        self.store_input = store_input
        self.experiment_id = experiment_id
        self.iteration_id = iteration_id
        self.source_node_id = source_node_id
        self.table = None
        self.batches = None
        self.threads = []
        self.completed_threads = 0
        self.lock = threading.Lock()

    def start(self):
        print(f"WoolMilk source node running at {self.location}")
        self.serve()

    def do_action(self, context, action):
        if action.type == SourceNodeActions.GET_STATUS:
            yield flight.Result(self.current_status.encode("utf-8"))
        elif action.type == SourceNodeActions.GENERATE_DATA:
            self.current_status = SourceNodeStatus.GENERATING_DATA
            self.generate_data()
            yield flight.Result(self.current_status.encode("utf-8"))
        elif action.type == SourceNodeActions.SEND_DATA:
            self.current_status = SourceNodeStatus.SENDING_DATA
            self.start_sending()
            yield flight.Result(self.current_status.encode("utf-8"))

    def generate_data(self):
        self.table = self.generate_table(
            num_rows=self.overall_tuples,
            event_type=self.event_type,
            generator_executable=self.generator_executable,
            offset=self.offset,
            step=self.step,
        )
        self.batches = self.table.to_batches(max_chunksize=self.tuples_per_batch)

        if self.store_input != "":
            input_file = Path(self.store_input)
            input_folder = input_file.parent
            os.makedirs(input_folder, exist_ok=True)
            table = pa.Table.from_batches(self.batches)
            pq.write_table(table, f"{input_file}")
            print(f"Wrote .. {input_file}")

        self.current_status = SourceNodeStatus.DATA_GENERATED

    def generate_table(
        self,
        num_rows=10**6,
        event_type="person",
        generator_executable="nexmark",
        offset=0,
        step=1,
    ):
        cmd = [
            generator_executable,
            "-n",
            str(num_rows),
            "--offset",
            str(offset),
            "--step",
            str(step),
            "--type",
            event_type,
            "--no-wait",
        ]
        print("Generate data..")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
        records = []

        try:
            if proc.stdout:
                for line in proc.stdout:
                    try:
                        record = json.loads(line)
                        key = event_type.capitalize()
                        records.append(record[key])
                    except json.JSONDecodeError:
                        continue
        finally:
            if proc.stdout:
                proc.stdout.close()
            proc.kill()
            proc.wait()

        print("Done.")
        return pa.Table.from_pylist(records)

    def start_sending(self):
        if self.table is None or self.batches is None:
            raise RuntimeError("Data not generated. Call GENERATE_DATA action first.")

        self.threads = []
        for thread_id in range(self.thread_count):
            t = threading.Thread(
                target=self.send_data,
                args=(
                    thread_id,
                    self.table.schema,
                    self.batches[thread_id :: self.thread_count],
                    self.processing_nodes,
                    self.experiment_id,
                    self.iteration_id,
                    self.source_node_id,
                ),
                daemon=True,
            )
            self.threads.append(t)
            t.start()

    def send_data(
        self,
        thread_id,
        schema,
        batches,
        processing_nodes,
        experiment_id=None,
        iteration_id=None,
        source_node_id=None,
    ):
        print(f"SEND_TIME: {time.time()}")
        path_info = {
            "experiment_id": experiment_id,
            "iteration_id": iteration_id,
            "source_node_id": source_node_id,
            "thread_id": thread_id,
        }
        encoded_path = json.dumps(path_info)

        writers = []
        for processing_node in processing_nodes:
            client = flight.FlightClient(
                f"grpc://{processing_node[0]}:{processing_node[1]}"
            )
            writer, _ = client.do_put(
                flight.FlightDescriptor.for_path(encoded_path), schema
            )
            writers.append(writer)

        start = time.time()
        send_times = []
        total_bytes = 0
        for i, batch in enumerate(batches):
            send_start = time.time()
            writers[(thread_id + i) % len(processing_nodes)].write_batch(batch)
            total_bytes += batch.nbytes
            send_end = time.time()
            send_times.append((send_start, send_end))

        for writer in writers:
            writer.done_writing()

        end = time.time()

        duration = end - start
        gbps = (total_bytes * 8) / (duration * 1000**3)
        mbps = total_bytes / (duration * 1000**2)

        log = {
            "thread": thread_id,
            "experiment_id": experiment_id,
            "iteration_id": iteration_id,
            "source_node_id": source_node_id,
            "send_times": send_times,
            "total_bytes": total_bytes,
            "start_time": start,
            "end_time": end,
            "gbps": f"{gbps:.4f}",
            "mbps": f"{mbps:.2f}",
        }

        print(f"WM_LOG= {json.dumps(log)}")

        with self.lock:
            self.completed_threads += 1
            if self.completed_threads == self.thread_count:
                self.current_status = SourceNodeStatus.DONE


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WoolMilk Source Node")
    parser.add_argument(
        "--stream",
        choices=["nexmark.bid", "nexmark.auction", "nexmark.person"],
        default="nexmark.person",
        help="Stream type",
    )
    parser.add_argument(
        "--generator-executable", default="nexmark", help="Executable to generate data"
    )
    parser.add_argument(
        "--overall-tuples",
        type=int,
        default=10**5,
        help="Total number of tuples needs to be sent.",
    )
    parser.add_argument(
        "--tuples-per-batch", type=int, default=10**4, help="Number of tuples per batch"
    )
    parser.add_argument(
        "--offset", type=int, default=0, help="Offset to start data generation"
    )
    parser.add_argument(
        "--step", type=int, default=1, help="Step for next tuple to generate"
    )
    parser.add_argument(
        "--processing-nodes",
        help="Flight server address (host:port,host:port)",
        type=str,
        default="localhost:8010",
    )
    parser.add_argument(
        "--thread-count",
        type=int,
        help="Number of threads to use for sending data",
        default=1,
    )
    parser.add_argument(
        "--store-input",
        type=str,
        help="Folder to store generated data",
        default="",
    )
    parser.add_argument(
        "--experiment-id",
        type=int,
        default=None,
        help="Experiment ID for logging metadata",
    )
    parser.add_argument(
        "--iteration-id",
        type=int,
        default=None,
        help="Iteration ID for logging metadata",
    )
    parser.add_argument(
        "--source-node-id",
        type=int,
        default=None,
        help="Unique ID for the source node",
    )
    parser.add_argument(
        "--source-server-address",
        type=str,
        default=None,
        help="Address where source flight server starts",
    )

    args = parser.parse_args()

    print("\n" + "=" * 40)
    print(" WoolMilk Source Node Parameters")
    print("=" * 40)
    print(f" Stream Type                : {args.stream}")
    print(f" Overall Tuples             : {args.overall_tuples}")
    print(f" Tuples Per Batch           : {args.tuples_per_batch}")
    print(f" Offset                     : {args.offset}")
    print(f" Step                       : {args.step}")
    print(f" Processing Nodes           : {args.processing_nodes}")
    print(f" Thread Count               : {args.thread_count}")
    print(f" Store Input                : {args.store_input}")
    print(f" Generator Executable       : {args.generator_executable}")
    print(f" Experiment ID              : {args.experiment_id}")
    print(f" Iteration ID               : {args.iteration_id}")
    print(f" Source Node ID             : {args.source_node_id}")
    print("=" * 40 + "\n")

    processing_nodes = []
    for address in args.processing_nodes.split(","):
        host, port = address.split(":")
        processing_nodes.append((host, int(port)))

    if len(processing_nodes) == 0:
        print("No server addresses for processing nodes provided. Exiting.")
        sys.exit(1)

    event_type = args.stream.split(".")[1]

    source_node = SourceNode(
        location=f"grpc://{args.source_server_address}",
        processing_nodes=processing_nodes,
        overall_tuples=args.overall_tuples,
        tuples_per_batch=args.tuples_per_batch,
        event_type=event_type,
        generator_executable=args.generator_executable,
        offset=args.offset,
        step=args.step,
        thread_count=args.thread_count,
        store_input=args.store_input,
        experiment_id=args.experiment_id,
        iteration_id=args.iteration_id,
        source_node_id=args.source_node_id,
    )

    source_node.start()
