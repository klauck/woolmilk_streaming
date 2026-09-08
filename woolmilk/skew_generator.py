import argparse
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from woolmilk.source_node import generate_table

_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"


def gini(values):
    """Gini coefficient of non-negative sizes (0 = uniform, ->1 = maximally
    skewed). Characterizes how concentrated the injected payload skew is."""
    v = np.sort(np.asarray(values, dtype=float))
    n = v.size
    if n == 0 or v.sum() == 0:
        return 0.0
    cum = np.cumsum(v)
    return (n + 1 - 2 * np.sum(cum) / cum[-1]) / n


def apply_payload_skew(
    table,
    field="extra",
    alpha=1.5,
    s_min=64,
    s_max=65536,
    seed=0,
    normalize_mean=None,
    correlate_with=None,
    placement="dispersed",
    hot_fraction=0.2,
    hot_at="middle",
    content_period=None,
    content_word=None,
):
    """Inflate a string `field` so tuple *payload sizes* follow a bounded
    Pareto (power-law) distribution: many small tuples, few very large ones.

    Parameters
    ----------
    field : name of a string column to inflate (default "extra", the
        Nexmark padding field present in bid/auction/person).
    alpha : Pareto tail index. Smaller = heavier tail = stronger skew.
    s_min, s_max : lower bound and hard cap (chars) for a single value;
        `s_max` keeps memory bounded.
    seed : RNG seed for reproducibility.
    normalize_mean : if set, sizes are rescaled so their mean approximates
        this target (for fair "skewed vs. uniform at equal total volume"
        comparisons). Approximate, since clipping to [s_min, s_max] shifts
        the mean slightly.
    correlate_with : name of an integer key column. If set, the (unchanged)
        Pareto size distribution is re-assigned so tuples with *hotter* keys
        receive the *larger* payloads -- payload size becomes positively
        correlated with key frequency (couples A2 to A1). The marginal size
        distribution is preserved exactly; only the row assignment changes.
        Apply key skew first so the key column is populated.
    placement : "dispersed" (default, i.i.d. -> the largest payloads are
        scattered so every batch is statistically similar) or "clustered"
        (the largest payloads are placed into one contiguous row window ->
        those batches become heavy: a temporal payload burst at the data
        level). Mutually exclusive with `correlate_with`.
    hot_fraction, hot_at : with placement="clustered", the fraction of rows in
        the hot window and its position ("start"/"middle"/"end").
    content_period : controls the *compressibility* of the generated content,
        orthogonal to size. None (default) -> fully random content (high
        entropy, incompressible; the size skew survives compression). An
        integer P -> a random P-char block tiled to fill each value (period
        P), so values compress to ~P regardless of size (small P = highly
        compressible). Lets you study payload size vs. compressibility as
        separate axes when transfer/storage compression is enabled.
    content_word : a concrete string (e.g. "Apple") tiled to fill each value
        instead of random characters -> readable, highly compressible content
        (period = word length). Takes precedence over `content_period`. None
        (default) keeps the random behavior above.

    Schema and row count are preserved, so the rest of the pipeline is
    unaffected.
    """
    idx = table.schema.get_field_index(field)
    if idx == -1:
        raise ValueError(
            f"field '{field}' not found in schema {table.schema.names}"
        )
    if not pa.types.is_string(table.schema.field(idx).type):
        raise ValueError(f"field '{field}' is not a string column")
    if placement not in ("dispersed", "clustered"):
        raise ValueError(
            f"placement must be 'dispersed' or 'clustered', got {placement!r}"
        )
    if correlate_with is not None and placement == "clustered":
        raise ValueError(
            "correlate_with and placement='clustered' are mutually exclusive"
        )
    if content_period is not None and content_period < 1:
        raise ValueError("content_period must be a positive integer or None")
    if content_word is not None and (
        not isinstance(content_word, str) or content_word == ""
    ):
        raise ValueError("content_word must be a non-empty string or None")

    rng = np.random.default_rng(seed)
    n = table.num_rows

    # bounded Pareto: classic Pareto(min=s_min) via Lomax, then clip to s_max
    sizes = (rng.pareto(alpha, n) + 1.0) * s_min
    if normalize_mean is not None:
        sizes *= normalize_mean / sizes.mean()
    sizes = np.clip(sizes, s_min, s_max).astype(int)

    corr = None
    if correlate_with is not None and n:
        kidx = table.schema.get_field_index(correlate_with)
        if kidx == -1:
            raise ValueError(
                f"correlate_with field '{correlate_with}' not found in "
                f"schema {table.schema.names}"
            )
        if not pa.types.is_integer(table.schema.field(kidx).type):
            raise ValueError(
                f"correlate_with field '{correlate_with}' is not an integer column"
            )
        keys = table.column(correlate_with).to_numpy()
        _, inv, cnt = np.unique(keys, return_inverse=True, return_counts=True)
        key_freq = cnt[inv].astype(float)
        # order rows by key frequency, then assign the sorted sizes so the
        # hottest-key rows receive the largest payloads. Preserves the size
        # marginal exactly -- only which row gets which size changes.
        order = np.argsort(key_freq + rng.random(n) * 1e-9, kind="stable")
        correlated = np.empty(n, dtype=sizes.dtype)
        correlated[order] = np.sort(sizes)
        sizes = correlated
        # Spearman rho between key frequency and payload size
        corr = float(
            np.corrcoef(
                np.argsort(np.argsort(key_freq)), np.argsort(np.argsort(sizes))
            )[0, 1]
        )

    hot_share = None
    if placement == "clustered" and n:
        hot_count = max(1, int(hot_fraction * n))
        if hot_at == "start":
            lo = 0
        elif hot_at == "end":
            lo = n - hot_count
        elif hot_at == "middle":
            lo = (n - hot_count) // 2
        else:
            raise ValueError(
                f"hot_at must be 'start', 'middle' or 'end', got {hot_at!r}"
            )
        # place the largest payloads into one contiguous window -> heavy
        # batches. Shuffle within the hot/cold regions so every batch in the
        # window is heavy (not just its first), rather than front-loading.
        sorted_desc = np.sort(sizes)[::-1]
        hot_sizes = sorted_desc[:hot_count].copy()
        cold_sizes = sorted_desc[hot_count:].copy()
        rng.shuffle(hot_sizes)
        rng.shuffle(cold_sizes)
        clustered = np.empty(n, dtype=sizes.dtype)
        mask = np.zeros(n, dtype=bool)
        mask[lo:lo + hot_count] = True
        clustered[mask] = hot_sizes
        clustered[~mask] = cold_sizes
        sizes = clustered
        hot_share = sizes[lo:lo + hot_count].sum() / sizes.sum()

    # Build one pool; each value is a prefix of it (fast). content_period
    # controls compressibility: None -> fully random pool (incompressible, so
    # the size skew survives compression). Integer P -> a random P-char block
    # tiled (period P) -> values compress to ~P regardless of size.
    if content_word is not None:
        reps = -(-int(s_max) // len(content_word))  # ceil division
        pool = (content_word * reps)[: int(s_max)]
    elif content_period is None:
        pool = "".join(_ALPHABET[i] for i in rng.integers(0, len(_ALPHABET), int(s_max)))
    else:
        block = "".join(
            _ALPHABET[i] for i in rng.integers(0, len(_ALPHABET), int(content_period))
        )
        reps = -(-int(s_max) // len(block))  # ceil division
        pool = (block * reps)[: int(s_max)]
    new_values = [pool[: int(s)] for s in sizes]

    table = table.set_column(idx, field, pa.array(new_values, type=pa.string()))

    top1 = np.sort(sizes)[int(0.99 * n):].sum() / sizes.sum() if n else 0
    if corr is not None:
        extra_str = f" corr({correlate_with})={corr:.2f}"
    elif hot_share is not None:
        extra_str = f" placement=clustered@{hot_at} hot_bytes={100 * hot_share:.0f}%"
    else:
        extra_str = ""
    if content_word is not None:
        extra_str += f" content_word={content_word!r}"
    elif content_period is not None:
        extra_str += f" content_period={content_period}"
    print(
        f"  payload-skew on '{field}': alpha={alpha} mean={sizes.mean():.0f} "
        f"max={sizes.max()} gini={gini(sizes):.3f} "
        f"top1%={100 * top1:.0f}% of bytes{extra_str}"
    )
    return table


def apply_key_skew(
    table,
    field="bidder",
    theta=1.2,
    num_keys=1000,
    seed=0,
):
    """Overwrite an integer key `field` so key *frequencies* follow a finite
    Zipf distribution over `num_keys` keys: a few hot keys dominate, most are
    rare. Models key/partition skew (A1). Key rank 0 is the hottest.
    """
    idx = table.schema.get_field_index(field)
    if idx == -1:
        raise ValueError(
            f"field '{field}' not found in schema {table.schema.names}"
        )
    if not pa.types.is_integer(table.schema.field(idx).type):
        raise ValueError(f"field '{field}' is not an integer column")

    rng = np.random.default_rng(seed)
    n = table.num_rows

    # finite Zipf: P(rank i) proportional to 1 / i^theta over num_keys keys
    ranks = np.arange(1, num_keys + 1)
    probs = np.power(ranks, -float(theta))
    probs /= probs.sum()
    keys = rng.choice(num_keys, size=n, p=probs)

    field_type = table.schema.field(idx).type
    table = table.set_column(idx, field, pa.array(keys, type=field_type))

    counts = np.bincount(keys, minlength=num_keys)
    distinct = int((counts > 0).sum())
    hottest = counts.max() / n if n else 0
    k1 = max(1, num_keys // 100)
    top1pct = np.sort(counts)[-k1:].sum() / n if n else 0
    print(
        f"  key-skew on '{field}': theta={theta} num_keys={num_keys} "
        f"distinct={distinct} gini={gini(counts):.3f} "
        f"hottest_key={100 * hottest:.0f}% top1%_keys={100 * top1pct:.0f}%"
    )
    return table


def apply_key_drift_skew(
    table,
    field="bidder",
    theta=1.2,
    num_keys=1000,
    num_phases=4,
    seed=0,
):
    """Overwrite an integer key `field` with a Zipf key distribution whose
    *dominant key drifts over time* (concept drift, A4). Rows are split into
    `num_phases` equal time segments (row order = send order = time); every
    phase keeps the same Zipf shape but shifts which key id is hottest.
    """
    idx = table.schema.get_field_index(field)
    if idx == -1:
        raise ValueError(
            f"field '{field}' not found in schema {table.schema.names}"
        )
    if not pa.types.is_integer(table.schema.field(idx).type):
        raise ValueError(f"field '{field}' is not an integer column")

    rng = np.random.default_rng(seed)
    n = table.num_rows

    ranks = np.arange(1, num_keys + 1)
    probs = np.power(ranks, -float(theta))
    probs /= probs.sum()

    keys = np.empty(n, dtype=np.int64)
    bounds = np.linspace(0, n, num_phases + 1).astype(int)
    hot_per_phase = []
    for p in range(num_phases):
        lo, hi = bounds[p], bounds[p + 1]
        if hi <= lo:
            continue
        drawn = rng.choice(num_keys, size=hi - lo, p=probs)
        shift = p * (num_keys // num_phases)  # hottest key marches across space
        seg = (drawn + shift) % num_keys
        keys[lo:hi] = seg
        hot_per_phase.append(int(np.bincount(seg, minlength=num_keys).argmax()))

    field_type = table.schema.field(idx).type
    table = table.set_column(idx, field, pa.array(keys, type=field_type))

    counts = np.bincount(keys, minlength=num_keys)
    print(
        f"  key-drift-skew on '{field}': theta={theta} num_keys={num_keys} "
        f"phases={num_phases} gini={gini(counts):.3f} "
        f"hot_key_per_phase={hot_per_phase}"
    )
    return table


def _dispatch_skew(table, spec):
    skew_type = spec.get("type")
    params = {k: v for k, v in spec.items() if k != "type"}
    if skew_type == "payload":
        return apply_payload_skew(table, **params)
    if skew_type == "key":
        return apply_key_skew(table, **params)
    if skew_type == "key_drift":
        return apply_key_drift_skew(table, **params)
    raise NotImplementedError(f"unknown skew type: {skew_type!r}")


def apply_skew(table, skew_config=None):
    """Apply a skew transformation to a generated Arrow table.

    `skew_config` is either a single skew spec, e.g.
    `{"type": "payload", "field": "extra", "alpha": 1.5, "s_max": 65536}`, or
    a LIST of specs applied in sequence to the same table -- e.g. key skew
    followed by key-correlated payload skew, combining A1 and A2 on one
    stream:
    `[{"type": "key", "field": "bidder", "theta": 1.5},
      {"type": "payload", "field": "extra", "alpha": 1.2,
       "correlate_with": "bidder"}]`.
    Returns the table unchanged if no config is given; raises on an unknown
    skew type so typos don't silently pass through.
    """
    if not skew_config:
        return table

    specs = skew_config if isinstance(skew_config, list) else [skew_config]
    for spec in specs:
        table = _dispatch_skew(table, spec)
    return table


def generate_skewed_table(
    number_of_tuples,
    stream,
    generator_executable,
    offset,
    step,
    skew_config=None,
):
    event_type = stream.split("_")[1]
    table = generate_table(
        number_of_tuples=number_of_tuples,
        stream=stream,
        generator_executable=generator_executable,
        offset=offset,
        step=step,
    )
    return apply_skew(table, skew_config.get(event_type) if skew_config else None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="WoolMilk Skew Generator: pre-generates (optionally skewed) "
        "Nexmark data as Parquet files for source nodes to pick up."
    )
    parser.add_argument(
        "--output-folder",
        default="input_data",
        help="Folder to write the generated Parquet files to",
    )
    parser.add_argument(
        "--stream",
        choices=["nexmark_bid", "nexmark_auction", "nexmark_person"],
        default="nexmark_person",
        help="Stream type",
    )
    parser.add_argument(
        "--generator-executable", default="nexmark", help="Executable to generate data"
    )
    parser.add_argument(
        "--overall-tuples",
        type=int,
        default=10**5,
        help="Total number of tuples to generate across all threads",
    )
    parser.add_argument(
        "--tuples-per-batch",
        type=int,
        default=10**4,
        help="Row group size of the resulting Parquet files "
        "(should match the source node's --tuples-per-batch)",
    )
    parser.add_argument(
        "--num-threads",
        type=int,
        default=1,
        help="Number of source node threads (i.e., processing nodes) "
        "the data will later be split across",
    )
    parser.add_argument(
        "--offset", type=int, default=0, help="Offset to start data generation"
    )
    parser.add_argument(
        "--step",
        type=int,
        default=-1,
        help="Step for next tuple to generate (defaults to --num-threads)",
    )
    parser.add_argument(
        "--skew-config",
        type=str,
        default=None,
        help=(
            "JSON config describing the desired skew, keyed by event type. "
            'Payload: \'{"bid": {"type": "payload", "field": "extra", '
            '"alpha": 1.5, "s_max": 65536}}\'. '
            'Key: \'{"bid": {"type": "key", "field": "bidder", '
            '"theta": 1.2, "num_keys": 1000}}\'. '
            'Key drift: \'{"bid": {"type": "key_drift", "field": "bidder", '
            '"theta": 1.2, "num_keys": 1000, "num_phases": 4}}\'. '
            "Omit for plain Nexmark data."
        ),
    )
    args = parser.parse_args()

    if args.step == -1:
        args.step = args.num_threads

    assert args.overall_tuples % args.num_threads == 0, (
        f"overall_tuples ({args.overall_tuples}) must be divisible by "
        f"num_threads ({args.num_threads})"
    )

    skew_config = json.loads(args.skew_config) if args.skew_config else None

    print("\n" + "=" * 40)
    print(" WoolMilk Skew Generator Parameters")
    print("=" * 40)
    print(f" Output Folder      : {args.output_folder}")
    print(f" Stream Type        : {args.stream}")
    print(f" Overall Tuples     : {args.overall_tuples}")
    print(f" Tuples Per Batch   : {args.tuples_per_batch}")
    print(f" Num Threads        : {args.num_threads}")
    print(f" Offset             : {args.offset}")
    print(f" Step               : {args.step}")
    print(f" Skew Config        : {skew_config}")
    print("=" * 40 + "\n")

    output_folder = Path(args.output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    tuples_per_thread = args.overall_tuples // args.num_threads

    for thread_id in range(args.num_threads):
        thread_offset = args.offset + thread_id

        table = generate_skewed_table(
            number_of_tuples=tuples_per_thread,
            stream=args.stream,
            generator_executable=args.generator_executable,
            offset=thread_offset,
            step=args.step,
            skew_config=skew_config,
        )

        path = (
            output_folder
            / f"{args.stream}_{tuples_per_thread}_{thread_offset}_{args.step}.parquet"
        )
        pq.write_table(
            table,
            path,
            row_group_size=args.tuples_per_batch,
            compression="snappy",
        )
        print(f"Wrote .. {path}")
