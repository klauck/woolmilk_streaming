import os
import shutil
import subprocess
import sys
import time
import unittest
from pathlib import Path

import datafusion
import pyarrow

import woolmilk.source_node


class TestQueries(unittest.TestCase):

    def setUp(self):
        current_dir = os.path.dirname(__file__)
        self.result_folder = os.path.join(current_dir, "test_nexmark_q2")
        if Path(self.result_folder).exists():
            shutil.rmtree(self.result_folder)
        self.overall_tuples = 1000

        self.woolmilk_dir = os.path.join(current_dir, "../woolmilk/")
        self.sink = subprocess.Popen(
            [
                "python",
                os.path.join(self.woolmilk_dir, "sink_node.py"),
                "--port",
                "8920",
                "--result-folder",
                self.result_folder,
            ]
        )
        self.addCleanup(self.cleanup_proc, self.sink)

        self.processing_node = subprocess.Popen(
            [
                "python",
                os.path.join(self.woolmilk_dir, "processing_node.py"),
                "--port",
                "8910",
                "--forward-node",
                "127.0.0.1:8920",
                "--query-result-schema",
                '{"fields":[{"name":"auction","type":"int64"},'
                '{"name":"price","type":"int64"}]}',
                "--query",
                "SELECT auction, price "
                "FROM nexmark_data "
                "WHERE auction = 1007 OR auction = 1020 "
                "OR auction = 2001 OR auction = 2019 OR auction = 2087",
            ]
        )
        self.addCleanup(self.cleanup_proc, self.processing_node)

        ctx = datafusion.SessionContext()
        bid = woolmilk.source_node.generate_table(self.overall_tuples, "nexmark_bid")
        ctx.register_record_batches("bid", [bid.to_batches()])
        self.expected_q2 = ctx.sql(
            "SELECT auction, price "
            "FROM Bid "
            "WHERE auction = 1007 OR auction = 1020 "
            "OR auction = 2001 OR auction = 2019 OR auction = 2087"
        ).collect()

        # Wait for processing node and sink to be ready to accept connections
        time.sleep(1)

    def cleanup_proc(self, proc):
        try:
            proc.terminate()
            proc.wait(timeout=2)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    def tearDown(self):
        # Remaining procs will be handled by addCleanup
        pass

    def test_single_processing_node(self):
        source = subprocess.Popen(
            [
                "python",
                os.path.join(self.woolmilk_dir, "source_node.py"),
                "--stream",
                "nexmark_bid",
                "--processing-nodes",
                "127.0.0.1:8910",
                "--overall-tuples",
                str(self.overall_tuples),
                "--tuples-per-batch",
                "100",
                "--input-folder",
                "../tests/input",
            ]
        )
        self.addCleanup(self.cleanup_proc, source)
        source.wait()
        
        time.sleep(1)

        # Compare expected and actual results
        ctx = datafusion.SessionContext()

        # Register actual results with potentially multiple parquet files)
        ctx.register_parquet("actual", self.result_folder)
        actual_batches = ctx.sql("SELECT * FROM actual").collect()
        actual_table = pyarrow.Table.from_batches(actual_batches)

        expected_table = pyarrow.Table.from_batches(self.expected_q2)
        self.assertTrue(actual_table.equals(expected_table))

    def test_single_processing_node_multiple_threads(self):
        source = subprocess.Popen(
            [
                "python",
                os.path.join(self.woolmilk_dir, "source_node.py"),
                "--stream",
                "nexmark_bid",
                "--processing-nodes",
                "127.0.0.1:8910,127.0.0.1:8910",
                "--overall-tuples",
                str(self.overall_tuples),
                "--tuples-per-batch",
                "100",
                "--input-folder",
                "../tests/input",
            ]
        )
        self.addCleanup(self.cleanup_proc, source)

        source.wait()
        
        time.sleep(1)

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
        processing_node2 = subprocess.Popen(
            [
                "python",
                os.path.join(self.woolmilk_dir, "processing_node.py"),
                "--port",
                "8911",
                "--forward-node",
                "127.0.0.1:8920",
                "--query-result-schema",
                '{"fields":[{"name":"auction","type":"int64"},'
                '{"name":"price","type":"int64"}]}',
                "--query",
                "SELECT auction, price "
                "FROM nexmark_data "
                "WHERE auction = 1007 OR auction = 1020 "
                "OR auction = 2001 OR auction = 2019 OR auction = 2087",
            ]
        )
        self.addCleanup(self.cleanup_proc, processing_node2)
        time.sleep(1)

        source = subprocess.Popen(
            [
                "python",
                os.path.join(self.woolmilk_dir, "source_node.py"),
                "--stream",
                "nexmark_bid",
                "--processing-nodes",
                "127.0.0.1:8910,127.0.0.1:8911",
                "--overall-tuples",
                str(self.overall_tuples),
                "--tuples-per-batch",
                "100",
                "--input-folder",
                "../tests/input",
            ]
        )
        self.addCleanup(self.cleanup_proc, source)
        
        source.wait()
        
        time.sleep(1)

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

    def test_two_sources(self):
        processing_node2 = subprocess.Popen(
            [
                "python",
                os.path.join(self.woolmilk_dir, "processing_node.py"),
                "--port",
                "8911",
                "--forward-node",
                "127.0.0.1:8920",
                "--query-result-schema",
                '{"fields":[{"name":"auction","type":"int64"},'
                '{"name":"price","type":"int64"}]}',
                "--query",
                "SELECT auction, price "
                "FROM nexmark_data "
                "WHERE auction = 1007 OR auction = 1020 "
                "OR auction = 2001 OR auction = 2019 OR auction = 2087",
            ]
        )
        self.addCleanup(self.cleanup_proc, processing_node2)
        time.sleep(1)

        number_of_source_nodes = 2
        number_of_processing_nodes = 2
        sources = []
        for source_id in range(number_of_source_nodes):
            server_address = f"127.0.0.1:{8210 + source_id}"
            source = subprocess.Popen(
                [
                    "python",
                    os.path.join(self.woolmilk_dir, "source_node.py"),
                    "--stream",
                    "nexmark_bid",
                    "--processing-nodes",
                    "127.0.0.1:8910,127.0.0.1:8911",
                    "--overall-tuples",
                    str(self.overall_tuples // number_of_source_nodes),
                    "--tuples-per-batch",
                    "50",
                    "--offset",
                    str(source_id * number_of_processing_nodes),
                    "--step",
                    str(number_of_source_nodes * number_of_processing_nodes),
                    "--input-folder",
                    "../tests/input",
                ]
            )

            self.addCleanup(self.cleanup_proc, source)
            sources.append(source)
            

        for source in sources:
            source.wait()
        
        time.sleep(1)

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
