import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc
from pyarrow import flight

DEFAULT_COLUMNS_TO_ENCODE = ["city", "name"]


def dictionary_encode_batch(
    batch: pa.RecordBatch, columns: list[str] = DEFAULT_COLUMNS_TO_ENCODE
) -> pa.RecordBatch:
    print("dictionary encoding batches...")
    new_arrays = []
    new_fields = []
    for i, field in enumerate(batch.schema):
        col = batch.column(i)
        if field.name in columns and (
            pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
        ):
            encoded = pc.dictionary_encode(col)
            new_arrays.append(encoded)
            new_fields.append(pa.field(field.name, encoded.type))
        else:
            new_arrays.append(col)
            new_fields.append(field)
    return pa.RecordBatch.from_arrays(new_arrays, schema=pa.schema(new_fields))


def dictionary_encode_schema(
    schema: pa.Schema, columns: list[str] = DEFAULT_COLUMNS_TO_ENCODE
) -> pa.Schema:
    print("dictionary encoding schema...")
    new_fields = []
    for field in schema:
        if field.name in columns and (
            pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
        ):
            new_fields.append(pa.field(field.name, pa.dictionary(pa.int32(), field.type)))
        else:
            new_fields.append(field)
    return pa.schema(new_fields)


def dictionary_decode_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    print("dictionary decoding batches...")
    # check if there is any dictionary encoded column
    has_dictionary_encoded_column = any(
        pa.types.is_dictionary(field.type) for field in batch.schema
    )
    if not has_dictionary_encoded_column:
        return batch

    new_arrays = []
    new_fields = []
    for i, field in enumerate(batch.schema):
        col = batch.column(i)
        if pa.types.is_dictionary(field.type):
            decoded = pc.dictionary_decode(col)
            new_arrays.append(decoded)
            new_fields.append(pa.field(field.name, decoded.type))
        else:
            new_arrays.append(col)
            new_fields.append(field)
    return pa.RecordBatch.from_arrays(new_arrays, schema=pa.schema(new_fields))


def get_compressed_flight_options(codec: str = "zstd") -> flight.FlightCallOptions:
    print(f"getting compressed flight options with codec: {codec}")
    write_options = ipc.IpcWriteOptions(compression=codec)
    return flight.FlightCallOptions(write_options=write_options)
