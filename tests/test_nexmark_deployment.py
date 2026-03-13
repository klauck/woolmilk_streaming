import shutil
import time
import unittest
from pathlib import Path

import datafusion
import pyarrow

import woolmilk.source_node
from woolmilk.control import prepare_source_nodes, start_sending, wait_until_completion
from woolmilk.run_cluster import DeploymentRunner, parse_config


class TestNexmarkDeployment(unittest.TestCase):

    def setUp(self):
        self.test_dir = Path(__file__).parent
        self.result_folder = self.test_dir / "results"
        if self.result_folder.exists():
            shutil.rmtree(self.result_folder)

    def deploy_and_wait(self, config_name):
        config = parse_config(self.test_dir / "configurations" / config_name)
        runner = DeploymentRunner(config, "logs")
        self.addCleanup(runner.cleanup)
        runner.deploy()

        prepare_source_nodes(config.source_nodes)
        start_sending(config.source_nodes)
        wait_until_completion(config.source_nodes)
        return config

    @staticmethod
    def _collect_parquet_results(folder: Path) -> pyarrow.Table:
        """Read and return all results from a folder of Parquet files."""
        ctx = datafusion.SessionContext()
        ctx.register_parquet("actual", str(folder))
        batches = ctx.sql("SELECT * FROM actual").collect()
        return pyarrow.Table.from_batches(batches)

    def _assert_tables_equal(self, actual: pyarrow.Table, expected: pyarrow.Table):
        """Compare two Arrow tables and show content on failure."""
        if not actual.equals(expected):
            print("\n=== Actual Table ===")
            print(actual)
            print("\n=== Expected Table ===")
            print(expected)
        self.assertTrue(actual.equals(expected), "Actual and expected tables differ.")

    def test_nexmark_Q1(self):
        self.deploy_and_wait("nexmark_Q1.json")

        # Collect actual results
        actual_table = self._collect_parquet_results(self.result_folder).sort_by(
            [
                ("auction", "ascending"),
                ("bidder", "ascending"),
                ("price", "ascending"),
                ("date_time", "ascending"),
            ]
        )

        # Calculate expected result
        ctx = datafusion.SessionContext()
        ctx.register_parquet("bid", self.test_dir / "input" / "test_Q1_bid")
        expected = ctx.sql(
            "SELECT auction, price * 0.85 AS price, bidder, date_time FROM Bid"
        ).collect()
        expected_table = pyarrow.Table.from_batches(expected).sort_by(
            [
                ("auction", "ascending"),
                ("bidder", "ascending"),
                ("price", "ascending"),
                ("date_time", "ascending"),
            ]
        )

        self._assert_tables_equal(actual_table, expected_table)

    def test_nexmark_Q2(self):
        self.deploy_and_wait("nexmark_Q2.json")

        # Collect actual results
        actual_table = self._collect_parquet_results(self.result_folder).sort_by(
            [("auction", "ascending"), ("price", "ascending")]
        )

        # Calculate expected result
        ctx = datafusion.SessionContext()
        bid = woolmilk.source_node.generate_table(1000, "nexmark_bid")
        ctx.register_record_batches("bid", [bid.to_batches()])
        expected = ctx.sql(
            "SELECT auction, price "
            "FROM Bid "
            "WHERE auction = 1007 OR auction = 1020 "
            "OR auction = 2001 OR auction = 2019 OR auction = 2087"
        ).collect()
        expected_table = pyarrow.Table.from_batches(expected).sort_by(
            [("auction", "ascending"), ("price", "ascending")]
        )

        self._assert_tables_equal(actual_table, expected_table)


if __name__ == "__main__":
    unittest.main()
