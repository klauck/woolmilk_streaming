import threading
import zlib
from collections import Counter
from functools import cache
from itertools import count

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import flight

from woolmilk.runtime_config import RuntimeConfig

SHIFT = pa.scalar(33, pa.uint64())
MULTIPLIER = pa.scalar(0xFF51AFD7ED558CCD, pa.uint64())
sent = Counter()
sent_lock = threading.Lock()


@cache
def client(address):
    return flight.FlightClient(f"grpc://{address}")


def targets(config, start_targets):
    return start_targets if config.forward_nodes is None else config.forward_nodes


def accept(raw):
    """A node never switches to a target that is down."""
    config = RuntimeConfig.from_json(raw)
    for address in config.forward_nodes or []:
        try:
            client(address).wait_for_available(timeout=2)
        except flight.FlightError:
            raise ValueError(f"unreachable: {address}")
    return config


def rows_sent():
    """Rows this node has sent to each target, reported by GET_INFO."""
    with sent_lock:
        return dict(sent)


def numbers(keys):
    """Lets hash also split on text columns like city or name."""
    if pa.types.is_integer(keys.type):
        return pc.cast(keys, pa.int64())
    hashes = [zlib.crc32(str(key).encode()) for key in keys.to_pylist()]
    return pa.array(hashes, pa.int64())


def mix(values):
    values = pc.bit_wise_xor(values, pc.shift_right(values, SHIFT))
    values = pc.multiply(values, MULTIPLIER)
    return pc.bit_wise_xor(values, pc.shift_right(values, SHIFT))


def seed(address):
    return pa.scalar(zlib.crc32(address.encode()), pa.uint64())


def by_hash(keys, targets):
    """Rendezvous hashing, so a new target only takes over the keys it wins."""
    keys = mix(numbers(keys).view(pa.uint64()))
    scores = [mix(pc.bit_wise_xor(keys, seed(address))) for address in targets]
    best = pc.max_element_wise(*scores)
    chosen = pa.scalar(0, pa.int64())
    for index, score in enumerate(scores):
        chosen = pc.if_else(pc.equal(score, best), pa.scalar(index, pa.int64()), chosen)
    return chosen


def by_range(keys, bounds):
    """Bounds [100, 200] split keys into below 100, 100 to 199 and the rest."""
    chosen = pa.scalar(0, pa.int64())
    for bound in bounds:
        reached = pc.greater_equal(keys, pa.scalar(bound, pa.float64()))
        chosen = pc.add(chosen, pc.cast(reached, pa.int64()))
    return chosen


class Router:
    """Picks targets for rows; round robin starts at an offset per sender."""

    def __init__(self, offset=0):
        self.turn = count(offset)

    def split(self, batch, config, targets):
        if len(targets) <= 1:
            return [(address, batch) for address in targets]
        if config.partitioning is None:
            return [(targets[next(self.turn) % len(targets)], batch)]
        keys = batch.column(config.partition_key)
        if config.partitioning == "hash":
            chosen = by_hash(keys, targets)
        else:
            chosen = by_range(keys, config.bounds)
        parts = [
            (address, batch.filter(pc.equal(chosen, pa.scalar(index, pa.int64()))))
            for index, address in enumerate(targets)
        ]
        return [(address, part) for address, part in parts if part.num_rows]


class Writer:
    """Stands in for a Flight writer and follows target changes batch by batch."""

    def __init__(self, descriptor, schema, options, current_config, start_targets, offset=0):
        self.descriptor = descriptor
        self.schema = schema
        self.options = options
        self.current_config = current_config
        self.start_targets = start_targets
        self.router = Router(offset)
        self.streams = {}

    def write_batch(self, batch):
        self.write_with_metadata(batch, None)

    def write_with_metadata(self, batch, metadata):
        config = self.current_config()
        chosen = targets(config, self.start_targets)
        for dropped in set(self.streams) - set(chosen):
            self.streams.pop(dropped).done_writing()
        for address, part in self.router.split(batch, config, chosen):
            if address not in self.streams:
                self.streams[address], _ = client(address).do_put(self.descriptor, self.schema, options=self.options)
            if metadata is None:
                self.streams[address].write_batch(part)
            else:
                self.streams[address].write_with_metadata(part, metadata)
            with sent_lock:
                sent[address] += part.num_rows

    def done_writing(self):
        for stream in self.streams.values():
            stream.done_writing()
