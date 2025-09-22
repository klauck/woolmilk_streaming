import os
import shutil
import time
import unittest
from pathlib import Path

import datafusion
import pyarrow

import woolmilk.source_node
from woolmilk.run_cluster import DeploymentRunner, parse_config


class TestNexmarkDeployment(unittest.TestCase):

    def setUp(self):
        self.test_dir = os.path.dirname(__file__)
        self.result_folder = os.path.join(self.test_dir, "results")
        if Path(self.result_folder).exists():
            shutil.rmtree(self.result_folder)

    def test_nexmark_Q1(self):
        config = parse_config(
            os.path.join(self.test_dir, "configurations/nexmark_Q1.json")
        )
        runner = DeploymentRunner(config, "logs")
        runner.deploy()
        time.sleep(2)
        runner.cleanup()

        # Compare expected and actual results
        ctx = datafusion.SessionContext()

        # Register actual results with potentially multiple parquet files)
        ctx.register_parquet("actual", self.result_folder)
        actual_batches = ctx.sql("SELECT * FROM actual").collect()
        actual_table = pyarrow.Table.from_batches(actual_batches).sort_by(
            [
                ("auction", "ascending"),
                ("bidder", "ascending"),
                ("price", "ascending"),
                ("date_time", "ascending"),
            ]
        )

        self.overall_tuples = 1000
        ctx = datafusion.SessionContext()
        ctx.register_parquet(
            "bid", Path(os.path.dirname(__file__)) / "input" / "test_Q1_bid.parquet"
        )
        expected = ctx.sql(
            "SELECT auction, price * 0.85 as price, bidder, date_time FROM Bid"
        ).collect()
        expected_table = pyarrow.Table.from_batches(expected).sort_by(
            [
                ("auction", "ascending"),
                ("bidder", "ascending"),
                ("price", "ascending"),
                ("date_time", "ascending"),
            ]
        )

        print(actual_table)
        print(expected_table)
        self.assertTrue(actual_table.equals(expected_table))

    def test_nexmark_Q2(self):
        config = parse_config(
            os.path.join(self.test_dir, "configurations/nexmark_Q2.json")
        )
        runner = DeploymentRunner(config, "logs")
        runner.deploy()
        time.sleep(2)
        runner.cleanup()

        # Compare expected and actual results
        ctx = datafusion.SessionContext()

        # Register actual results with potentially multiple parquet files)
        ctx.register_parquet("actual", self.result_folder)
        actual_batches = ctx.sql("SELECT * FROM actual").collect()
        actual_table = pyarrow.Table.from_batches(actual_batches).sort_by(
            [("auction", "ascending"), ("price", "ascending")]
        )

        ctx = datafusion.SessionContext()
        bid = woolmilk.source_node.generate_table(1000, "bid")
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

        self.assertTrue(actual_table.equals(expected_table))


if __name__ == "__main__":
    unittest.main()
