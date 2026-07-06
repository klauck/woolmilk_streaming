#!/usr/bin/env python3
import json
import os

from pyflink.table import EnvironmentSettings, TableEnvironment

SCHEMA_DDL = """
    id BIGINT,
    name STRING,
    email_address STRING,
    credit_card STRING,
    city STRING,
    state STRING,
    date_time BIGINT,
    extra STRING
"""


def source_ddl(table, path):
    return f"""
        CREATE TABLE {table} (
        {SCHEMA_DDL}
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{path}',
            'format' = 'csv'
        )
    """


def sink_ddl(table, path):
    return f"""
        CREATE TABLE {table} (
        {SCHEMA_DDL}
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{path}',
            'format' = 'csv'
        )
    """


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "config.json")) as f:
        cfg = json.load(f)

    settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("parallelism.default", "2")
    t_env.get_config().set("pipeline.operator-chaining", "false")

    stmt_set = t_env.create_statement_set()

    for i, pipe in enumerate(cfg["pipelines"], start=1):
        src = f"person_source_{i}"
        snk = f"sink_{i}"
        t_env.execute_sql(source_ddl(src, pipe["source_csv"]))
        t_env.execute_sql(sink_ddl(snk, pipe["sink_dir"]))
        stmt_set.add_insert_sql(f"INSERT INTO {snk} {pipe['query']}")
        print(f"wired {src} -> [{pipe['query']}] -> {snk}")

    stmt_set.execute().wait()
    print("job finished")


if __name__ == "__main__":
    main()
