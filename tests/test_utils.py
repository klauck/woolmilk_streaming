import os
from pathlib import Path
import shutil
import subprocess
from typing import Dict
import datafusion
import pyarrow
from woolmilk.run_cluster import parse_config
from woolmilk.source_node import generate_table


class TestUtil():
    __test__ = False  # Tell pytest this is not a test class
    
    def __init__(self, test_name: str, overall_tuples: int = 1000, results_folder: str = "results"):
        self.test_dir = os.path.dirname(__file__)
        self.result_folder = os.path.join(self.test_dir, results_folder)
        self.woolmilk_dir = os.path.join(self.test_dir, "../woolmilk/")
        self.ctx = datafusion.SessionContext()
        self.overall_tuples = overall_tuples
        self.configs = {}

    def register_config(self, name: str, config_file: str):
        self.config_path = os.path.join(self.test_dir, f"configurations/{config_file}")
        assert Path(self.config_path).exists(), "Configuration file does not exist"
        
        self.configs[name] = parse_config(self.config_path)

    def get_config(self, name: str):
        assert name in self.configs, "Configuration not registered"
        return self.configs[name]

    def setup(self):
        if Path(self.result_folder).exists():
            shutil.rmtree(self.result_folder)
            
        os.makedirs(self.result_folder, exist_ok=True)

    def register_results_folder(self):
        self.ctx.register_parquet("actual", self.result_folder)

    def load_table(self, source: str):
        """Load a table into the context.

        Args:
            source (str): The name of the source table to load. Must be one of "bid", "auction", "person", or "category".
        """
        assert source in ["bid", "auction", "person", "category"], "Invalid source"

        if source == "category":
            # category is derived from auction table
            result_df = self.ctx.sql("SELECT DISTINCT(category) AS id FROM auction").collect()
            self.ctx.register_record_batches("category", [result_df])
            return

        table = generate_table(self.overall_tuples, source)
        self.ctx.register_record_batches(source, [table.to_batches()])

    def load_table_from_dir(self, source: str, dir_path: str):
        self.ctx.register_parquet(source, Path(self.test_dir) / dir_path)
    
    def load_parquet(self, table_name: str, parquet_path: str):
        self.ctx.register_parquet(table_name, parquet_path)

    def show_sql_result(self, query: str):
        """Show the result of a SQL query.

        Args:
            query (str): The SQL query to execute.
        """
        result = self.ctx.sql(query)
        result.show()

    def sql(self, query, sort=None):
        """
        Execute a SQL query and return the result as a PyArrow Table.
        If the query is already a PyArrow Table, it will be sorted if specified.

        Args:
            query (str | pyarrow.Table): The SQL query to execute or a PyArrow Table.
            sort (list[tuple[str, str]], optional): A list of columns to sort by. Defaults to None.

        Returns:
            pyarrow.Table: The result of the query as a PyArrow Table.
        """
        if isinstance(query, str):
            result = self.ctx.sql(query)
            table = pyarrow.Table.from_batches(result.collect())
        elif isinstance(query, pyarrow.Table):
            # query is already a PyArrow Table
            table = query
        else:
            # query is a result from previous sql call (list of batches)
            table = pyarrow.Table.from_batches(query)
        
        if sort:
            table = table.sort_by(sort)

        return table
    
    def new_python_process(self, source: str, commands: Dict[str, str]) -> subprocess.Popen:
        """Create a new Python process.

        Args:
            source (str): The source file to execute.
            commands (Dict[str, str]): The command-line arguments to pass to the process.

        Returns:
            subprocess.Popen: The subprocess.Popen instance for the new process.
        """
        cmd = ["python", os.path.join(self.woolmilk_dir, source)]
        for key, value in commands.items():
            cmd.append(f"--{key}")
            if value:
                cmd.append(value)
        return subprocess.Popen(cmd)