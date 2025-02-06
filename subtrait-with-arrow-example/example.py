import pyarrow as pa
from pyarrow.lib import tobytes
import pyarrow.substrait as substrait
test_table_1 = pa.Table.from_pydict({"users": [1, 2, 3]})
test_table_2 = pa.Table.from_pydict({"x": [4, 5, 6]})
def table_provider(names, schema):
    if not names:
       raise Exception("No names provided")
    elif names[0] == "t1":
       return test_table_1
    elif names[1] == "t2":
       return test_table_2
    else:
       raise Exception("Unrecognized table name")

substrait_query = '''
        {
            "relations": [
            {"rel": {
                "read": {
                "base_schema": {
                    "struct": {
                    "types": [
                                {"i64": {}}
                            ]
                    },
                    "names": [
                            "x"
                            ]
                },
                "namedTable": {
                        "names": ["t1"]
                }
                }
            }}
            ]
        }
'''

buf = pa._substrait._parse_json_plan(tobytes(substrait_query))
reader = pa.substrait.run_query(buf, table_provider=table_provider)

print(reader.read_all())
