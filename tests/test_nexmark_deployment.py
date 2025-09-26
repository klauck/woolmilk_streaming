import time
import unittest
from tests.test_utils import TestUtil
from woolmilk.run_cluster import DeploymentRunner


class TestNexmarkDeployment(unittest.TestCase):

    def setUp(self):
        self.util = TestUtil("nexmark_deployment", overall_tuples=1000)
        self.util.setup()
        self.util.register_config("nexmark_Q1", "nexmark_Q1.json")
        self.util.register_config("nexmark_Q2", "nexmark_Q2.json")

    def run_deployment_test(self, config_name: str):
        config = self.util.get_config(config_name)
        runner = DeploymentRunner(config, "logs")
        runner.deploy()
        time.sleep(2)
        runner.cleanup()
        self.util.register_results_folder()

    def run_q1_test(self):
        self.run_deployment_test("nexmark_Q1")

        sort_tables = [
            ("auction", "ascending"),
            ("bidder", "ascending"),
            ("price", "ascending"),
            ("date_time", "ascending"),
        ]

        actual_table = self.util.sql("SELECT * FROM actual", sort_tables)

        self.util.load_table_from_dir("bid", "input/test_Q1_bid.parquet")
        expected_table = self.util.sql(
            "SELECT auction, price * 0.85 as price, bidder, date_time FROM Bid",
            sort=sort_tables,
        )

        print(actual_table)
        print(expected_table)
        self.assertTrue(actual_table.equals(expected_table))

    def run_q2_test(self):
        self.run_deployment_test("nexmark_Q2")

        sort_tables = [("auction", "ascending"), ("price", "ascending")]

        actual_table = self.util.sql("SELECT * FROM actual", sort_tables)

        # Generate data for Q2
        self.util.load_table("bid")
        expected_table = self.util.sql(
            "SELECT auction, price "
            "FROM Bid "
            "WHERE auction = 1007 OR auction = 1020 "
            "OR auction = 2001 OR auction = 2019 OR auction = 2087",
            sort=sort_tables,
        )

        print(actual_table)
        print(expected_table)
        self.assertTrue(actual_table.equals(expected_table))

    def test_nexmark_Q1(self):
        self.run_q1_test()

    def test_nexmark_Q2(self):
        self.run_q2_test()

if __name__ == "__main__":
    unittest.main()
