"""Generate WoolMilk cluster + benchmark configs from a topology string.

Define nodes in NODES (name -> type + address), draw the graph in TOPOLOGY,
then run this file. No CLI args.

TOPOLOGY syntax:
    - one flow per line, left to right, groups joined by '->'
    - commas group nodes; between two adjacent groups every left node
      connects to every right node (cross product)
    - repeating a target on a source line = one source thread each, so
      'sN -> pX, pX, pX' spawns 3 threads all sending to pX (data partitioned)
    - across lines the count is the max seen on any single line, so writing
      the same edge on separate rows does NOT add threads
    - '#' starts a comment, blank lines ignored

    s1,s2 -> p1,p2        both sources feed both processing nodes
    s1 -> p1, p1, p1      one source, 3 threads all into p1
    p1 -> p3              chains allowed (processing -> processing)
    p3,p4 -> sn1          many processing -> one sink (convergence ok)

Wiring rule (WoolMilk runtime limit):
    - source.processing_nodes is a list (may repeat) -> a source MAY fan out
      and MAY run multiple threads to the same node
    - processing.forward_node is single -> a processing node MUST have exactly
      one distinct outgoing edge; fan-out at the processing/sink layer errors
"""
import argparse
import json
import os

QUERY_RESULT_SCHEMA = {
    "fields": [
        {"name": "id", "type": "int64"},
        {"name": "name", "type": "string"},
        {"name": "email_address", "type": "string"},
        {"name": "credit_card", "type": "string"},
        {"name": "city", "type": "string"},
        {"name": "state", "type": "string"},
        {"name": "date_time", "type": "int64"},
        {"name": "extra", "type": "string"},
    ]
}

NODES = {
    "s1": {"type": "source", "addr": "127.0.0.1:8210"},
    "s2": {"type": "source", "addr": "127.0.0.1:8211"},
    "p1": {"type": "processing", "addr": "127.0.0.1:8815"},
    "p2": {"type": "processing", "addr": "127.0.0.1:8816"},
    "p3": {"type": "processing", "addr": "127.0.0.1:8817"},
    "p4": {"type": "processing", "addr": "127.0.0.1:8818"},
    "sn1": {"type": "sink", "addr": "127.0.0.1:8820"},
    "sn2": {"type": "sink", "addr": "127.0.0.1:8821"},
}

TOPOLOGY = """
s1,s2 -> p1,p2
p1 -> sn1
p2 -> sn2
"""

SETTINGS = {
    "stream": "nexmark_person",
    "overall_tuples": 1_000_000,
    "tuples_per_batch": 50_000,
    "query": "SELECT * FROM nexmark_data WHERE name > 'H'",
    "batches_per_second": None,
    "compression": None,
    "encoding": None,
    "columns_to_encode": ["city", "name"],
    "use_buffering": False,
    "result_folder": "results",
    "input_folder": "input_data",
    "store_input": True,
    "generator_executable": "nexmark",
    "iterations": 3,
}

REMOTE_SERVERS = {}

BATCH_BPS_MATRIX = [
    (50000, "10"),
    (25000, "20"),
    (10000, "50"),
    (50000, None),
    (25000, None),
    (10000, None),
]

OPT_MODES = [
    {"comp": None, "enc": None},
    {"comp": None, "enc": "dictionary"},
    {"comp": "zstd", "enc": None},
    {"comp": "zstd", "enc": "dictionary"},
]

QUERIES = [
    ("gtH", "SELECT * FROM nexmark_data WHERE name > 'H'"),
    ("ltH", "SELECT * FROM nexmark_data WHERE name < 'H'"),
]


def opt_name(opt):
    parts = []
    if opt["comp"]:
        parts.append("comp")
    if opt["enc"]:
        parts.append("enc")
    return "_".join(parts) or "none"


def host_of(address):
    return address.split(":")[0]


def parse_topology(text, nodes):
    counts = {}
    for lineno, raw in enumerate(text.splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        layers = [seg.strip() for seg in line.split("->")]
        if len(layers) < 2:
            raise ValueError(f"line {lineno}: expected at least one '->': {raw.strip()!r}")
        groups = []
        for seg in layers:
            names = [n.strip() for n in seg.split(",") if n.strip()]
            if not names:
                raise ValueError(f"line {lineno}: empty group in {raw.strip()!r}")
            for n in names:
                if n not in nodes:
                    raise ValueError(f"line {lineno}: unknown node {n!r} (not in NODES)")
            groups.append(names)
        line_counts = {}
        for left, right in zip(groups, groups[1:]):
            for l in left:
                for r in right:
                    line_counts[(l, r)] = line_counts.get((l, r), 0) + 1
        for edge, c in line_counts.items():
            counts[edge] = max(counts.get(edge, 0), c)
    return counts


def build_graph(counts, nodes):
    out = {n: [] for n in nodes}
    incoming = {n: [] for n in nodes}
    for l, r in sorted(counts):
        c = counts[(l, r)] if nodes[l]["type"] == "source" else 1
        out[l].extend([r] * c)
        if l not in incoming[r]:
            incoming[r].append(l)

    for n, meta in nodes.items():
        t = meta["type"]
        o = out[n]
        distinct = sorted(set(o))
        i = incoming[n]
        if t == "source":
            if i:
                raise ValueError(f"source {n} has incoming edges {i}: sources are entry points")
            if not o:
                raise ValueError(f"source {n} has no outgoing edge")
            for d in distinct:
                if nodes[d]["type"] != "processing":
                    raise ValueError(f"source {n} -> {d}: a source must feed a processing node")
        elif t == "processing":
            if not i:
                raise ValueError(f"processing {n} has no incoming edge (nothing feeds it)")
            if len(distinct) == 0:
                raise ValueError(f"processing {n} has no forward target")
            if len(distinct) > 1:
                raise ValueError(
                    f"processing {n} forwards to {distinct}: forward_node is single-target, "
                    f"only sources may fan out"
                )
            if nodes[distinct[0]]["type"] == "source":
                raise ValueError(f"processing {n} -> {distinct[0]}: cannot forward to a source")
        elif t == "sink":
            if o:
                raise ValueError(f"sink {n} has outgoing edges {distinct}: sinks are terminal")
            if not i:
                raise ValueError(f"sink {n} has no incoming edge (nothing writes to it)")
        else:
            raise ValueError(f"node {n}: unknown type {t!r}")
    return out


def nodes_of_type(nodes, t):
    return [n for n, meta in nodes.items() if meta["type"] == t]


def build_cluster(nodes, out, s):
    addr = lambda n: nodes[n]["addr"]

    sink_nodes = [
        {"server_address": addr(n), "result_folder": s["result_folder"]}
        for n in nodes_of_type(nodes, "sink")
    ]
    processing_nodes = [
        {"server_address": addr(n), "forward_node": addr(out[n][0])}
        for n in nodes_of_type(nodes, "processing")
    ]

    config = {}
    if REMOTE_SERVERS:
        config["config"] = {"remote_servers": REMOTE_SERVERS}
    config["sink_nodes"] = sink_nodes
    config["processing_nodes"] = processing_nodes
    return config


def build_benchmark(nodes, out, s):
    addr = lambda n: nodes[n]["addr"]
    procs = nodes_of_type(nodes, "processing")
    sinks = nodes_of_type(nodes, "sink")
    sources = nodes_of_type(nodes, "source")

    source_templates = []
    for n in sources:
        source_templates.append(
            {
                "processing_nodes": [addr(d) for d in out[n]],
                "stream": s["stream"],
                "overall_tuples": s["overall_tuples"],
                "tuples_per_batch": s["tuples_per_batch"],
                "server_address": addr(n),
                "deployment_server": host_of(addr(n)),
                "generator_executable": s["generator_executable"],
                "input_folder": s["input_folder"],
                "store_input": s["store_input"],
            }
        )

    cluster_nodes = [
        {"node_type": "processing", "server_address": addr(n)} for n in procs
    ] + [{"node_type": "sink", "server_address": addr(n)} for n in sinks]

    node_configs = {}
    experiments = []
    included = list(range(len(sources)))
    for batch, bps in BATCH_BPS_MATRIX:
        for opt in OPT_MODES:
            for tag, sql in QUERIES:
                profile = {}
                for n in procs:
                    cfg = {
                        "compression": opt["comp"],
                        "encoding": opt["enc"],
                        "use_buffering": s["use_buffering"],
                        "tuples_per_batch": batch,
                        "query": sql,
                        "query_result_schema": QUERY_RESULT_SCHEMA,
                    }
                    if opt["enc"]:
                        cfg["columns_to_encode"] = s["columns_to_encode"]
                    profile[addr(n)] = cfg
                params = {
                    "tuples_per_batch": batch,
                    "batches_per_second": bps,
                    "compression": opt["comp"],
                    "encoding": opt["enc"],
                }
                if opt["enc"]:
                    params["columns_to_encode"] = s["columns_to_encode"]
                batch_k = batch // 1000
                bps_str = f"{bps}bps" if bps is not None else "maxbps"
                name = f"{bps_str}_{batch_k}k_{opt_name(opt)}_{tag}"
                node_configs[name] = profile
                experiments.append(
                    {
                        "included_nodes": included,
                        "iterations": s["iterations"],
                        "overridden_params": [params for _ in included],
                        "name": name,
                        "node_config": name,
                    }
                )

    config = {}
    if REMOTE_SERVERS:
        config["config"] = {"remote_servers": REMOTE_SERVERS}
    config["source_nodes"] = source_templates
    config["node_configs"] = node_configs
    config["experiments"] = experiments
    config["cluster_nodes"] = cluster_nodes
    return config


def print_topology(nodes, out):
    print("topology:")
    for n in nodes:
        seen = {}
        for d in out[n]:
            seen[d] = seen.get(d, 0) + 1
        for d, c in seen.items():
            threads = f" x{c} threads" if c > 1 else ""
            print(f"  {n:>4} ({nodes[n]['addr']}) -> {d} ({nodes[d]['addr']}){threads}")


def write(config, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(config, f, indent=3)
    print(f"wrote {path}")


def parse_args():
    ap = argparse.ArgumentParser(description="Generate WoolMilk cluster + benchmark configs from a topology string")
    ap.add_argument("--config-name", dest="config_name", default="custom")
    ap.add_argument("--out-dir", dest="out_dir", default="scripts/generated-configs")
    return ap.parse_args()


def main():
    args = parse_args()
    edges = parse_topology(TOPOLOGY, NODES)
    used = {n for edge in edges for n in edge}
    active = {n: NODES[n] for n in NODES if n in used}
    unused = [n for n in NODES if n not in used]
    if unused:
        print(f"note: NODES not used in TOPOLOGY, ignored: {unused}")

    out = build_graph(edges, active)
    print_topology(active, out)

    write(build_cluster(active, out, SETTINGS), f"{args.out_dir}/cluster-{args.config_name}.json")
    write(build_benchmark(active, out, SETTINGS), f"{args.out_dir}/benchmark-{args.config_name}.json")


if __name__ == "__main__":
    main()
