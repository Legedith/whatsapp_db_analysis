import argparse
import glob
import os
import sqlite3
from typing import List


def find_dbs(root: str, pattern: str) -> List[str]:
    return glob.glob(os.path.join(root, pattern), recursive=True)


def list_tables(db_path: str) -> List[str]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    conn.close()
    return tables


def print_sample(db_path: str, table: str, cols: List[str], limit: int):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    col_sql = ",".join(cols)
    q = f"SELECT {col_sql} FROM {table} ORDER BY rowid DESC LIMIT {limit}"
    print(q)
    for r in cur.execute(q):
        print(r)
    conn.close()


def main():
    p = argparse.ArgumentParser(description="Inspect SQLite WhatsApp DBs")
    p.add_argument("root", nargs="?", default=".", help="root folder to search")
    p.add_argument("--pattern", default="**/msgstore.db", help="glob pattern to find DBs")
    p.add_argument("--list-tables", action="store_true", help="list tables in found DBs")
    p.add_argument("--table", default="message", help="table to sample")
    p.add_argument("--cols", default="_id,chat_row_id,from_me,text_data,timestamp", help="columns to select (comma separated)")
    p.add_argument("--limit", type=int, default=10, help="number of rows to print")

    args = p.parse_args()

    dbs = find_dbs(args.root, args.pattern)
    print("FOUND", dbs)

    for db in dbs:
        print(f"\n--- OPENING {db}")
        try:
            if args.list_tables:
                print("TABLES:", list_tables(db))
            else:
                cols = [c.strip() for c in args.cols.split(",") if c.strip()]
                print_sample(db, args.table, cols, args.limit)
        except Exception as e:
            print("ERROR opening", db, e)


if __name__ == "__main__":
    main()
