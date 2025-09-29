
import time
import unittest

from tests.test_utils import TestUtil

PROCESSING_NODE_FILE = "processing_node.py"
SINK_NODE_FILE = "sink_node.py"
SOURCE_NODE_FILE = "source_node.py"

class TestQueries(unittest.TestCase):
    def setUp(self):
        self.util = TestUtil("TestQueries", overall_tuples=1000, results_folder="test_nexmark_q2")
        self.util.setup()

        self.sink = self.util.new_python_process(SINK_NODE_FILE, {
            "port": "8920",
            "result-folder": self.util.result_folder,
        })

        self.processing_node = self.util.new_python_process(PROCESSING_NODE_FILE, {
            "port": "8910",
            "forward-node": "127.0.0.1:8920",
            "query-result-schema": '{"fields":[{"name":"auction","type":"int64"},'
            '{"name":"price","type":"int64"}]}',
            "query": "SELECT auction, price "
            "FROM nexmark_data "
            "WHERE auction = 1007 OR auction = 1020 "
            "OR auction = 2001 OR auction = 2019 OR auction = 2087"
        })

        self.util.load_table("bid")
        self.expected_q2 = ("SELECT auction, price "
            "FROM Bid "
            "WHERE auction = 1007 OR auction = 1020 "
            "OR auction = 2001 OR auction = 2019 OR auction = 2087")

        # Wait for processing node and sink to be ready to accept connections
        time.sleep(1)

    def tearDown(self):
        self.processing_node.terminate()
        self.processing_node.wait()

        self.sink.terminate()
        self.sink.wait()

    def test_single_processing_node(self):
        source = self.util.new_python_process(SOURCE_NODE_FILE, {
            "stream": "nexmark.bid",
            "processing-nodes": "127.0.0.1:8910",
            "overall-tuples": str(self.util.overall_tuples),
            "tuples-per-batch": "100",
        })

        source.wait()
        time.sleep(1)

        self.util.register_results_folder()
        actual_table = self.util.sql("SELECT * FROM actual")
        expected_table = self.util.sql(self.expected_q2)

        self.assertTrue(actual_table.equals(expected_table))

    def test_single_processing_node_multiple_threads(self):
        source = self.util.new_python_process(SOURCE_NODE_FILE, {
            "stream": "nexmark.bid",
            "processing-nodes": "127.0.0.1:8910",
            "overall-tuples": str(self.util.overall_tuples),
            "tuples-per-batch": "100",
            "thread-count": "3",
        })

        source.wait()

        time.sleep(1)

        self.util.register_results_folder()
        actual_table = self.util.sql("SELECT * FROM actual", 
            [("auction", "ascending"), ("price", "ascending")])
    
        expected_table = self.util.sql(self.expected_q2,
            [("auction", "ascending"), ("price", "ascending")])

        print(actual_table)
        print(expected_table)
        self.assertTrue(actual_table.equals(expected_table))

    def test_two_processing_node_multiple_threads(self):
        processing_node2 = self.util.new_python_process(PROCESSING_NODE_FILE, {
            "port": "8911",
            "forward-node": "127.0.0.1:8920",
            "query-result-schema": '{"fields":[{"name":"auction","type":"int64"},'
            '{"name":"price","type":"int64"}]}',
            "query": "SELECT auction, price "
            "FROM nexmark_data "
            "WHERE auction = 1007 OR auction = 1020 "
            "OR auction = 2001 OR auction = 2019 OR auction = 2087",
        })
        
        time.sleep(1)

        source = self.util.new_python_process(SOURCE_NODE_FILE, {
            "stream": "nexmark.bid",
            "processing-nodes": "127.0.0.1:8910,127.0.0.1:8911",
            "overall-tuples": str(self.util.overall_tuples),
            "tuples-per-batch": "100",
            "thread-count": "3",
        })
        
        source.wait()

        time.sleep(1)
        processing_node2.terminate()
        processing_node2.wait()

        
        self.util.register_results_folder()
        actual_table = self.util.sql("SELECT * FROM actual",
            [("auction", "ascending"), ("price", "ascending")])

        expected_table = self.util.sql(self.expected_q2,
            [("auction", "ascending"), ("price", "ascending")])

        print(actual_table)
        print(expected_table)

        self.assertTrue(actual_table.equals(expected_table))

    def test_two_sources(self):
        processing_node2 = self.util.new_python_process(PROCESSING_NODE_FILE, {
            "port": "8911",
            "forward-node": "127.0.0.1:8920",
            "query-result-schema": '{"fields":[{"name":"auction","type":"int64"},'
            '{"name":"price","type":"int64"}]}',
            "query": "SELECT auction, price "
            "FROM nexmark_data "
            "WHERE auction = 1007 OR auction = 1020 "
            "OR auction = 2001 OR auction = 2019 OR auction = 2087",
        })
        
        time.sleep(1)

        number_of_source_nodes = 2
        sources = []
        for source_id in range(number_of_source_nodes):
            source = self.util.new_python_process(SOURCE_NODE_FILE, {
                "stream": "nexmark.bid",
                "processing-nodes": "127.0.0.1:8910,127.0.0.1:8911",
                "overall-tuples": str(self.util.overall_tuples // number_of_source_nodes),
                "tuples-per-batch": "100",
                "thread-count": "3",
                "offset": str(source_id),
                "step": str(number_of_source_nodes),
            })
            
            sources.append(source)
        for source in sources:
            source.wait()

        time.sleep(1)
        processing_node2.terminate()
        processing_node2.wait()

        self.util.register_results_folder()
        actual_table = self.util.sql("SELECT * FROM actual",
            [("auction", "ascending"), ("price", "ascending")]
        )

        expected_table = self.util.sql(self.expected_q2,
            [("auction", "ascending"), ("price", "ascending")]
        )

        print(actual_table)
        print(expected_table)

        self.assertTrue(actual_table.equals(expected_table))

if __name__ == "__main__":
    unittest.main()
