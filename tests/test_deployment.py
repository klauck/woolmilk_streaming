import time
import unittest
from tests.test_utils import TestUtil
from woolmilk.run_cluster import DeploymentRunner


class TestDeployment(unittest.TestCase):
    def setUp(self):
        self.util = TestUtil("test_deployment", overall_tuples=1000)
        self.util.setup()
        self.util.load_table("bid")
        self.expected_q2 = self.util.sql(
            "SELECT auction, price "
            "FROM Bid "
            "WHERE auction = 1007 OR auction = 1020 "
            "OR auction = 2001 OR auction = 2019 OR auction = 2087"
        )

        self.util.register_config("single_processing_node", "single_processing_node.json")
        self.util.register_config("two_processing_nodes", "two_processing_nodes.json")

    def run_test(self, config_name: str):
        config = self.util.get_config(config_name)
        runner = DeploymentRunner(config, "logs")
        runner.deploy()
        time.sleep(2)
        runner.cleanup()

        self.util.register_results_folder()

        actual_table = self.util.sql(
            "SELECT * FROM actual", [("auction", "ascending"), ("price", "ascending")]
        )

        expected_table = self.expected_q2.sort_by(
            [("auction", "ascending"), ("price", "ascending")]
        )

        print(actual_table)
        print(expected_table)
        self.assertTrue(actual_table.equals(expected_table))

    def test_single_processing_node(self):
        self.run_test("single_processing_node")

    def test_two_processing_nodes(self):
        self.run_test("two_processing_nodes")


if __name__ == "__main__":
    unittest.main()
