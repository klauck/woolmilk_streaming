import os
import subprocess
import time
import unittest


class TestQueries(unittest.TestCase):

    def setUp(self):
        current_dir = os.path.dirname(__file__)
        self.woolmilk_dir = os.path.join(current_dir, "../woolmilk/")
        self.sink = subprocess.Popen(
            ["python", os.path.join(self.woolmilk_dir, "sink_node.py"), "--port", "8920"]
        )
        self.processing_node = subprocess.Popen(
            [
                "python",
                os.path.join(self.woolmilk_dir, "processing_node.py"),
                "--port",
                "8910",
                "--forward-node",
                "127.0.0.1:8920",
                "--query-result-schema",
                '{"fields":[{"name":"id","type":"int64"},{"name":"name","type":"string"},{"name":"email_address","type":"string"},{"name":"credit_card","type":"string"},{"name":"city","type":"string"},{"name":"state","type":"string"},{"name":"date_time","type":"int64"},{"name":"extra","type":"string"}]}',
            ]
        )
        time.sleep(1)

    def tearDown(self):
        self.processing_node.terminate()
        self.processing_node.wait()

        self.sink.terminate()
        self.sink.wait()

    def test_single_processing_node(self):
        source = subprocess.Popen(
            [
                "python",
                os.path.join(self.woolmilk_dir, "source_node.py"),
                "--processing-nodes",
                "127.0.0.1:8910",
                "--overall-tuples",
                "200",
                "--tuples-per-batch",
                "100",
            ]
        )
        source.wait()


if __name__ == "__main__":
    unittest.main()
