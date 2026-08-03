import os

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, RollingPolicy
from pyflink.common.serialization import Encoder

from person import make_person, name_gt_h

N = 100000
RESULTS = "/opt/flink-datastream/results"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_python_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "person.py"))
    env.set_parallelism(1)

    # Disable operator chaining to ensure that each operator runs in its own task slot
    env.disable_operator_chaining()

    #each machine = 1 slot sharing group, so that each operator runs in its own task slot/machine

    src = (
        env.from_collection(list(range(N)), type_info=Types.LONG())
        .slot_sharing_group("source")
        .map(make_person, output_type=Types.STRING())
        .slot_sharing_group("source")
        .name("source")
    )

    proc = src.rebalance().filter(name_gt_h).slot_sharing_group("proc").name("processing")

    sink = (
        FileSink.for_row_format(RESULTS, Encoder.simple_string_encoder())
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .build()
    )
    
    proc.rebalance().sink_to(sink).slot_sharing_group("sink").name("sink")

    env.execute("datastream-generator")


if __name__ == "__main__":
    main()
