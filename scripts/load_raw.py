import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "warehouse.duckdb"
DATA_DIR = ROOT / "data"

FILES = {
    "raw_customers": "customers.csv",
    "raw_products": "products.csv",
    "raw_orders": "orders.csv",
    "raw_order_items": "order_items.csv",
}

def main():
    con = duckdb.connect(str(DB_PATH))

    for table in FILES.keys():
        con.execute(f"DROP TABLE IF EXISTS {table};")

    for table, filename in FILES.items():
        path = DATA_DIR / filename

        con.execute(f"""
            CREATE TABLE {table} AS
            SELECT *
            FROM read_csv_auto('{path.as_posix()}', header=true, all_varchar=true);
        """)

    for table in FILES.keys():
        n = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        print(f"{table}: {n} rows")

    con.close()
    print("Raw load complete.")

if __name__ == "__main__":
    main()