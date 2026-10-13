"""Generate WoolMilk cluster + benchmark configs from a JSON spec.

All configurable values live in an input JSON spec (see scripts/config-gen-spec.json
for a template). Run:

    python scripts/generate-config.py <spec.json> -o <output-dir> [--config-name NAME]

Spec keys:
    nodes                name -> {"type": source|processing|sink, "addr": "host:port"}
    topology             flow string, or list of flow lines (see syntax below)
    settings             stream / tuples / query defaults / iterations / folders ...
    query_result_schema  arrow schema forwarded by processing nodes
    remote_servers       host -> {username, base_dir, python_env} (optional, {} to skip)
    batch_bps_matrix     [[tuples_per_batch, batches_per_second|null], ...]
    opt_modes            [{"comp": null|"zstd"|"lz4", "enc": null|"dictionary", "buffering": bool}, ...]
    queries              [[tag, sql], ...]

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

SPEC_KEYS = [
    "nodes", "topology", "settings", "query_result_schema",
    "remote_servers", "batch_bps_matrix", "opt_modes", "queries",
]


def opt_name(opt):
    if opt["buffering"]:
        base_name = "buffered_"
    else:
        base_name = "unbuffered_"
    parts = []
    if opt["comp"]:
        parts.append("comp")
    if opt["enc"]:
        parts.append("enc")
    compression_name = "_".join(parts) or "none"
    return base_name + compression_name


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
                    raise ValueError(f"line {lineno}: unknown node {n!r} (not in nodes)")
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


def build_cluster(nodes, out, spec):
    s = spec["settings"]
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
    if spec["remote_servers"]:
        config["config"] = {"remote_servers": spec["remote_servers"]}
    config["sink_nodes"] = sink_nodes
    config["processing_nodes"] = processing_nodes
    return config


def build_benchmark(nodes, out, spec):
    s = spec["settings"]
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
    for batch, bps in spec["batch_bps_matrix"]:
        for opt in spec["opt_modes"]:
            for tag, sql in spec["queries"]:
                profile = {}
                for n in procs:
                    cfg = {
                        "compression": opt["comp"],
                        "encoding": opt["enc"],
                        "use_buffering": opt["buffering"],
                        "tuples_per_batch": batch,
                        "query": sql,
                        "query_result_schema": spec["query_result_schema"],
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
    if spec["remote_servers"]:
        config["config"] = {"remote_servers": spec["remote_servers"]}
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
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(config, f, indent=3)
    print(f"wrote {path}")


def load_spec(path):
    with open(path) as f:
        spec = json.load(f)
    missing = [k for k in SPEC_KEYS if k not in spec]
    if missing:
        raise ValueError(f"spec {path} missing keys: {missing}")
    return spec


def parse_args():
    ap = argparse.ArgumentParser(
        description="Generate WoolMilk cluster + benchmark configs from a JSON spec"
    )
    ap.add_argument("spec", help="input JSON spec (see scripts/config-gen-spec.json)")
    ap.add_argument("-o", "--output", dest="output", required=True, help="output directory")
    ap.add_argument(
        "--config-name", dest="config_name", default=None,
        help="base name for output files (default: spec filename without extension)",
    )
    return ap.parse_args()


def main():
    args = parse_args()
    spec = load_spec(args.spec)
    config_name = args.config_name or os.path.splitext(os.path.basename(args.spec))[0]

    nodes = spec["nodes"]
    topology = spec["topology"]
    if isinstance(topology, list):
        topology = "\n".join(topology)
    edges = parse_topology(topology, nodes)
    used = {n for edge in edges for n in edge}
    active = {n: nodes[n] for n in nodes if n in used}
    unused = [n for n in nodes if n not in used]
    if unused:
        print(f"note: nodes not used in topology, ignored: {unused}")

    out = build_graph(edges, active)
    print_topology(active, out)

    write(build_cluster(active, out, spec), f"{args.output}/cluster-{config_name}.json")
    write(build_benchmark(active, out, spec), f"{args.output}/benchmark-{config_name}.json")


if __name__ == "__main__":
    main()
