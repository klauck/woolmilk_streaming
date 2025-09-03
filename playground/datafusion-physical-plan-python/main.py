from json import load
from datafusion import ExecutionPlan, SessionContext
import pyarrow as pa
from pathlib import Path

execution_plan_proto_path = "execution_plan.pb"
data_csv_path = "countries.csv"
QUERY = "SELECT country, population FROM countries WHERE population > 1000"

def register_data(ctx: SessionContext):
    ctx.register_csv("countries", data_csv_path)

def create_save_execution_plan(ctx: SessionContext):
    df = ctx.sql(QUERY)
    exec_plan = df.execution_plan()
    batches = ctx.execute(exec_plan, 5)
    for batch in batches:
        print(batch.to_pandas())
    
    print("Execution Plan:")
    print(exec_plan.display_indent())

    print("Saving execution plan to:", execution_plan_proto_path)
    pb = exec_plan.to_proto()
    with open(execution_plan_proto_path, "wb") as f:
        f.write(pb)

def load_execution_plan(ctx: SessionContext):
    print("Loading execution plan from:", execution_plan_proto_path)
    with open(execution_plan_proto_path, "rb") as f:
        pb = f.read()
    
    exec_plan = ExecutionPlan.from_proto(ctx, pb)
    return exec_plan


def main():
    ctx = SessionContext()
    
    register_data(ctx)

    execution_plan_file = Path(execution_plan_proto_path)
    if not execution_plan_file.exists():
        print(f"Execution plan file {execution_plan_proto_path} does not exist. Creating it.")
        create_save_execution_plan(ctx)

    plan = load_execution_plan(ctx)
    batches = ctx.execute(plan, 0) # 0 partitions

    print("Execution completed. Result batches: ")

    for batch in batches:
        pyarrow_batch = batch.to_pyarrow()
        pandas_df = pyarrow_batch.to_pandas()
        print(pandas_df)

    print("\nQuery comparison:")
    ctx.sql(QUERY).show()

if __name__ == "__main__":
    main()