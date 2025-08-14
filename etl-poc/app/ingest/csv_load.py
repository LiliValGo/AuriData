import csv, sys
from app.ingest.loader import insert_batch
from app.config import settings
def main(path: str, table: str, chunk_size: int = None):
    chunk_size = chunk_size or settings.batch_size
    total, invalid = 0, 0
    batch = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            batch.append(row)
            if len(batch) >= chunk_size:
                res = insert_batch(table, batch)
                total += res["inserted"]
                invalid += res["invalid"]
                batch = []
        if batch:
            res = insert_batch(table, batch)
            total += res["inserted"]
            invalid += res["invalid"]
    print({"inserted_total": total, "invalid_total": invalid})
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python -m app.ingest.csv_load <csv_path> <table>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
