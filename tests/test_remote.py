import shutil
import time
import unittest
from pathlib import Path

from woolmilk.run_cluster import DeploymentRunner, parse_config


class TestRemote(unittest.TestCase):

    def setUp(self):
        current_dir = Path(__file__).parent
        self.test_dir = current_dir / "test_remote"

        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

        self.test_dir.mkdir(parents=True)

        self.config_path = (
            Path(__file__).parent.parent / "scripts" / "docker-config-remote.json"
        )
        self.logs_dir = self.test_dir / "logs"
        self.results_dir = self.test_dir / "results"

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

        runner.deploy()
        time.sleep(5)
        runner.cleanup()

        log_files = list(self.logs_dir.glob("**/*.log"))
        self.assertGreater(len(log_files), 0)

    def test_remote_deployment_with_results(self):
        config = parse_config(self.config_path)
        """passing self.test_dir here instead of self.results_dir, 
        because results are automatically stored under root/results 
        so passing results dir will create results/results"""
        runner = DeploymentRunner(
            config,
            "logs",
            mode="remote",
            log_to_file=True,
            local_results_dir=str(self.test_dir),
        )

        runner.deploy()
        time.sleep(5)
        runner.cleanup()

        result_files = list(self.results_dir.glob("*.parquet"))
        print(f"No of results files: {len(result_files)}")
        self.assertGreater(len(result_files), 0)


if __name__ == "__main__":
    unittest.main()
