import pyarrow as pa
import pyarrow.flight as fl
import pandas as pd

if __name__ == "__main__":
    client = fl.FlightClient("grpc://localhost:8815")

    user_descripter = fl.FlightDescriptor.for_command("users")
    flight_info = client.get_flight_info(user_descripter)

    ticket = flight_info.endpoints[0].ticket

    reader = client.do_get(ticket)
    table = reader.read_all()

    print("Received table from server:")
    print(table.to_pandas())

    # create new users table with the same schema
    new_table = pa.Table.from_pandas(
        pd.DataFrame({
            "name": ["Halfpap"],
            "id": [5]
        }),
        schema=table.schema
    )

    # send the new table to the server
    writer, reader = client.do_put(user_descripter, table.schema)
    writer.write_table(new_table) #we also have write_batch to write the data in chunks
    writer.close()

    # custom action on the server
    get_users_ids_action = fl.Action("get_users_ids", b"Send Me The Users IDs")
    itr_result = client.do_action(get_users_ids_action)

    for result in itr_result:
        # result.body is a pyarrow Buffer
        print("Server response:", result.body.to_pybytes().decode("utf-8"))
