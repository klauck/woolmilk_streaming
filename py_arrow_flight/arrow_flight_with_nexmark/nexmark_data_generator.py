import subprocess
import json
import random
import string
import pyarrow as pa

class NexmarkDataGenerator:
    def __init__(self, chunk_size, no_records):
        self.chunk_size = chunk_size
        self.no_records = no_records

    @staticmethod
    def random_text(max_length):
        length = random.randint(1, max_length)
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def get_schemas(self):
        # Category table schema
        category_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])

        # Person table schema
        person_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("email_address", pa.string()),
            pa.field("credit_card", pa.string()),
            pa.field("city", pa.string()),
            pa.field("state", pa.string()),
            pa.field("date_time", pa.int64()),
            pa.field("extra", pa.string())
        ])

        # Auction table schema
        auction_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("item_name", pa.string()),
            pa.field("description", pa.string()),
            pa.field("initial_bid", pa.int64()),
            pa.field("reserve", pa.int64()),
            pa.field("date_time", pa.int64()),
            pa.field("expires", pa.int64()),
            pa.field("seller", pa.int64()),
            pa.field("category", pa.int64()),
            pa.field("extra", pa.string())
        ])

        # Bid table schema
        bid_schema = pa.schema([
            pa.field("auction", pa.int64()),
            pa.field("bidder", pa.int64()),
            pa.field("price", pa.int64()),
            pa.field("channel", pa.string()),
            pa.field("url", pa.string()),
            pa.field("date_time", pa.int64()),
            pa.field("extra", pa.string())
        ])

        return category_schema, person_schema, auction_schema, bid_schema


    def generate_categories(self, raw_records):
        ids = set()
        for rec in raw_records:
            if isinstance(rec, dict):
                for content in rec.values():
                    if isinstance(content, dict) and 'category' in content:
                        try:
                            ids.add(int(content['category']))
                        except (ValueError, TypeError):
                            continue
        
        for cid in sorted(ids):
            raw_records.append({
                "Category": {"id": cid,
                             "name": self.random_text(15)}
            })
        return raw_records

    def to_arrow_table(self, records, key):
        pylist = [rec[key] for rec in records if key in rec]
        if not pylist:
            return pa.Table.from_pydict({})
        return pa.Table.from_pylist(pylist)

    def generate(self):
        cmd = ["nexmark", "-n", str(self.no_records), "--no-wait"]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
        buffer = []
        count = 0
        try:
            for line in proc.stdout:
                if count >= self.no_records:
                    break
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                buffer.append(record)
                count += 1
                if len(buffer) >= self.chunk_size:
                    chunk = self.generate_categories(buffer)
                    # build tables
                    person_tbl = self.to_arrow_table(chunk, 'Person')
                    auction_tbl = self.to_arrow_table(chunk, 'Auction')
                    bid_tbl = self.to_arrow_table(chunk, 'Bid')
                    category_tbl = self.to_arrow_table(chunk, 'Category')
                    yield person_tbl, auction_tbl, bid_tbl, category_tbl
                    buffer = []
            if buffer:
                chunk = self.generate_categories(buffer)
                yield (
                    self.to_arrow_table(chunk, 'Person'),
                    self.to_arrow_table(chunk, 'Auction'),
                    self.to_arrow_table(chunk, 'Bid'),
                    self.to_arrow_table(chunk, 'Category')
                )
        finally:
            try:
                proc.stdout.close()
            except Exception:
                pass
            proc.kill()
            proc.wait()


if __name__ == "__main__":
    gen = NexmarkDataGenerator(chunk_size=1000, no_records=1001)
    for person_tbl, auction_tbl, bid_tbl, category_tbl in gen.generate():
        print(f"Person table: {person_tbl.num_rows} rows")
        print(f"Auction table: {auction_tbl.num_rows} rows")
        print(f"Bid table: {bid_tbl.num_rows} rows")
        print(f"Category table: {category_tbl.num_rows} rows")