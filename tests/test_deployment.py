import os
import shutil
import time
import unittest
from pathlib import Path

import datafusion
import pyarrow

import woolmilk.source_node
from woolmilk.control import prepare_source_nodes, start_sending, wait_until_completion
from woolmilk.run_cluster import DeploymentRunner, parse_config


class TestDeployment(unittest.TestCase):

    def setUp(self):
        self.test_dir = os.path.dirname(__file__)
        self.overall_tuples = 1000
        ctx = datafusion.SessionContext()
        bid = woolmilk.source_node.generate_table(self.overall_tuples, "nexmark_bid")
        ctx.register_record_batches("bid", [bid.to_batches()])
        self.expected_q2 = ctx.sql(
            "SELECT auction, price "
            "FROM Bid "
            "WHERE auction = 1007 OR auction = 1020 "
            "OR auction = 2001 OR auction = 2019 OR auction = 2087"
        ).collect()
        self.result_folder = os.path.join(self.test_dir, "results")
        if Path(self.result_folder).exists():
            shutil.rmtree(self.result_folder)
            
    def deploy_and_wait(self, config_path):
        config = parse_config(config_path)
        runner = DeploymentRunner(config, "logs")
        self.addCleanup(runner.cleanup)
        runner.deploy()
        
        prepare_source_nodes(config.source_nodes)
        start_sending(config.source_nodes)
        wait_until_completion(config.source_nodes)
        return config

    def test_single_processing_node(self):
        self.deploy_and_wait(os.path.join(self.test_dir, "configurations/single_processing_node.json"))

        # Compare expected and actual results
        ctx = datafusion.SessionContext()

        # Register actual results with potentially multiple parquet files)
        ctx.register_parquet("actual", self.result_folder)
        actual_batches = ctx.sql("SELECT * FROM actual").collect()
        actual_table = pyarrow.Table.from_batches(actual_batches).sort_by(
            [("auction", "ascending"), ("price", "ascending")]
        )

        expected_table = pyarrow.Table.from_batches(self.expected_q2).sort_by(
            [("auction", "ascending"), ("price", "ascending")]
        )

        print(actual_table)
        print(expected_table)
        self.assertTrue(actual_table.equals(expected_table))

    def test_two_processing_nodes(self):
        self.deploy_and_wait(os.path.join(self.test_dir, "configurations/two_processing_nodes.json"))

        # Compare expected and actual results
        ctx = datafusion.SessionContext()

        # Register actual results with potentially multiple parquet files)
        ctx.register_parquet("actual", self.result_folder)
        actual_batches = ctx.sql("SELECT * FROM actual").collect()
        actual_table = pyarrow.Table.from_batches(actual_batches).sort_by(
            [("auction", "ascending"), ("price", "ascending")]
        )

        expected_table = pyarrow.Table.from_batches(self.expected_q2).sort_by(
            [("auction", "ascending"), ("price", "ascending")]
        )

        print(actual_table)
        print(expected_table)
        self.assertTrue(actual_table.equals(expected_table))


if __name__ == "__main__":
    unittest.main()
