# flink-test — WoolMilk topology in PyFlink

A minimal Apache Flink (PyFlink) reproduction of the WoolMilk
`source → processing → sink` shape, for comparison. Two independent pipelines
run inside one Flink job:

```
data/person_1.csv → source_1 → SQL: WHERE name > 'H' → sink_1 → results/sink_1/
data/person_2.csv → source_2 → SQL: WHERE name > 'H' → sink_2 → results/sink_2/
```

## Mapping to WoolMilk

| WoolMilk                        | Here                                       |
| ------------------------------- | ------------------------------------------ |
| `source_node` (Arrow Flight)    | filesystem CSV source table                |
| `processing_node` (DataFusion)  | per-pipeline Flink SQL query               |
| `sink_node` (Parquet writer)    | filesystem CSV sink table                  |
| `scripts/config.json`           | `config.json` (pipelines array)            |
| person schema                   | same 8 columns, `data_generator.py:28`     |

## Quickstart

```bash
./run.sh
```

This builds the PyFlink Docker image, starts a JobManager + TaskManager, and
submits the job (reading the static CSVs in `data/`). Flink UI at http://localhost:8081.

Stop the cluster with `docker compose down`.

## Files

- `config.json` — per-pipeline `source_csv` / `query` / `sink_dir`.
- `data/person_{1,2}.csv` — static input (person schema, no header). Edit directly.
- `job.py` — Table API job: 2 source + 2 sink tables, `StatementSet` with 2 INSERTs,
  batch mode, parallelism 2.
- `Dockerfile` — `flink:1.20.0` + Python + `apache-flink==1.20.0`.
- `docker-compose.yml` — JobManager + TaskManager, mounts this dir at `/opt/flink-test`.

## Notes

- CSVs have **no header row** — Flink's filesystem CSV format has no skip-header
  option, so a header would be parsed as data.
- Operator chaining is disabled (`pipeline.operator-chaining=false`) so the job
  graph shows 6 distinct nodes — 2 sources, 2 processing (`Calc`, the SQL query),
  2 filesystem sinks — instead of fusing each source→filter→sink chain into one task.
- Batch mode is used because the CSV input is bounded (no checkpointing needed to
  roll sink files).
- Change the filter / projection by editing `query` in `config.json`. A query must
  select the full person schema so the columns match the sink table.
