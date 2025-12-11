import json
import subprocess
from typing import Iterator, Dict, Any, Generator

import pyarrow
from pyarrow import RecordBatch


# Hella slow this function
def nexmark_event_generator_live(stream_type: str, generator_executable: str) -> Iterator[Dict[str, Any]]:
    """""
    Starting Nexmark as own Process to generade continues data
    """""
    cmd = [
        generator_executable,
        "--type", stream_type,
        "--format", "json",
    ]

    print(f"Starting Nexmark: {' '.join(cmd)}")
    Nexmark_Process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    if Nexmark_Process.stdout is None:
        print("Failed to start Nexmark: no stdout")
        return

    try:
        for line in Nexmark_Process.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            record = obj.get(stream_type.capitalize())
            if record is not None:
                yield record
    finally:
        try:
            Nexmark_Process.terminate()
        except Exception:
            pass

SHUTDOWN = False

def nexmark_event_generator_fixed_table(stream_type: str, generator_executable: str, number_rows: int
) -> Iterator[Dict[str, Any]]:
    cmd = [
        generator_executable,
        "-n",
        str(number_rows),
        "--type",
        stream_type,
        "--no-wait",
    ]
    print("Nexmark Event Generator Fixed Table..")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)

    try:
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            record = obj.get(stream_type.capitalize())
            if record is not None:
                yield record
    finally:
        proc.stdout.close()
        proc.kill()



def nexmark_event_generator_table(
    stream_type: str,
    generator_executable: str,
    preload_count: int = 10**5
):
    cmd = [
        generator_executable,
        "--type", stream_type,
        "--format", "json",
        "-n", str(preload_count),
        "--no-wait",
    ]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    records = []

    try:
        for line in proc.stdout:
            try:
                record = json.loads(line)
                key = stream_type.capitalize()
                records.append(record[key])
            except json.JSONDecodeError:
                continue
    finally:
        proc.stdout.close()
        proc.kill()
        proc.wait()


    return records


def nexmark_event_generator_fake_live(
    stream_type: str,
    generator_executable: str,
        schema: pyarrow.Schema
) -> Iterator[tuple]:
    records = nexmark_event_generator_table(stream_type, generator_executable)
    # Batches 1000 a 1000 Tuplen

    names = schema.names
    records2 = [
        tuple(rec[name] for name in names)
        for rec in records
    ]


    idx = 0
    N = len(records2)

    while True: #2ms
        yield records2[idx]
        idx = (idx + 1) % N

    ### Das soll Batches liefern (direkt mit tuple per seconds)

    # Parquet Format.
    # Nexmark in Parquet umwandeln, und das hier einlesen und in Arrow umwandeln

    # Parquet File
    # Jeder batch muss beim yield timestamp aufs aktuelle sezten.

    # Overall tuples als argument, um zu finishen. bei -1 wäre das unendlich


    # 1. Nexmark generiert millionen tupels ins parquet File
    # 2. Parquet in Arrow umwandeln
    # 3. Wir lesen die parquet Files und generieren daraus die Batches.
    # Parquet in Arrow umwandlung auch Benchmarken






