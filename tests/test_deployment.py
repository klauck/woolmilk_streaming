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

    def wait_until_results_are_ready(self):
        print(f"Waiting for results in {self.result_folder}...")
        start_time = time.time()
        timeout = 30
        while time.time() - start_time < timeout:
            if os.path.exists(self.result_folder) and any(f.endswith('.parquet') for f in os.listdir(self.result_folder)):
                break
            print("Waiting for results...")
            time.sleep(1)
            
    def test_single_processing_node(self):
        config = parse_config(
            os.path.join(self.test_dir, "configurations/single_processing_node.json")
        )
        runner = DeploymentRunner(config, "logs")
        runner.deploy()
        
        prepare_source_nodes(config.source_nodes)
        start_sending(config.source_nodes)
        wait_until_completion(config.source_nodes)

        self.wait_until_results_are_ready()

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
        
        runner.cleanup()

    def test_two_processing_nodes(self):
        config = parse_config(
            os.path.join(self.test_dir, "configurations/two_processing_nodes.json")
        )
        runner = DeploymentRunner(config, "logs")
        runner.deploy()
        
        prepare_source_nodes(config.source_nodes)
        start_sending(config.source_nodes)
        wait_until_completion(config.source_nodes)

        self.wait_until_results_are_ready()

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
        
        runner.cleanup()


if __name__ == "__main__":
    unittest.main()
