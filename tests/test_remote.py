import os
import shutil
import time
import unittest
from pathlib import Path

import datafusion
import pyarrow

import woolmilk.source_node
from woolmilk.run_cluster import DeploymentRunner, parse_config


@unittest.skipUnless(
    os.getenv("WOOLMILK_DOCKER") == "true", "Skipping unless WOOLMILK_DOCKER is set"
)
class TestRemote(unittest.TestCase):

    def setUp(self):
        current_dir = Path(__file__).parent
        self.test_dir = current_dir / "test_remote"
        self.overall_tuples = 1000

        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

        self.test_dir.mkdir(parents=True)

        self.config_path = current_dir / "configurations" / "docker-config-remote.json"
        self.logs_dir = self.test_dir / "logs"
        self.results_dir = self.test_dir / "results"

        ctx = datafusion.SessionContext()
        person = woolmilk.source_node.generate_table(self.overall_tuples, "person")
        ctx.register_record_batches("person", [person.to_batches()])
        self.expected_result = ctx.sql("SELECT * FROM person WHERE name > 'H'").collect()

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_remote_deployment_with_logs(self):
        config = parse_config(self.config_path)
        runner = DeploymentRunner(
            config,
            "logs",
            mode="remote",
            log_to_file=True,
            local_log_dir=str(self.logs_dir),
        )
        self.addCleanup(runner.cleanup)

        runner.deploy()
        time.sleep(2)

        log_files = list(self.logs_dir.glob("**/*.log"))
        self.assertGreater(len(log_files), 0)

    def test_remote_deployment_with_results(self):
        config = parse_config(self.config_path)

        runner = DeploymentRunner(
            config,
            log_dir="logs",
            mode="remote",
            log_to_file=True,
            local_results_dir=str(self.results_dir),
        )
        self.addCleanup(runner.cleanup)

        runner.deploy()
        time.sleep(2)

        result_files = list(self.results_dir.glob("**/*.parquet"))
        self.assertGreater(len(result_files), 0)

        ctx = datafusion.SessionContext()
        ctx.register_parquet("actual", str(self.results_dir / "results_test_docker"))
        actual_batches = ctx.sql("SELECT * FROM actual").collect()
        actual_table = pyarrow.Table.from_batches(actual_batches)

        expected_table = pyarrow.Table.from_batches(self.expected_result)

        self.assertEqual(actual_table.num_rows, expected_table.num_rows)
