import argparse
import csv
import os
import sqlite3
from typing import List, Optional


def export_messages(src: str, out: str, cols: List[str], chat_id: Optional[int], order: str):
    os.makedirs(os.path.dirname(out) or '.', exist_ok=True)
    conn = sqlite3.connect(src)
    cur = conn.cursor()

    existing = [c[1] for c in cur.execute("PRAGMA table_info('message')")]
    export_cols = [c for c in cols if c in existing]
    if not export_cols:
        print('NO_COLUMNS')
        conn.close()
        return

    where = ''
    params = []
    if chat_id is not None:
        where = 'WHERE chat_row_id = ?'
        params = [chat_id]

    order_sql = 'ASC' if order.lower() == 'asc' else 'DESC'
    q = f"SELECT {','.join(export_cols)} FROM message {where} ORDER BY timestamp {order_sql}"

    cnt = 0
    with open(out, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(export_cols)
        for row in cur.execute(q, params):
            # normalize None -> '' for CSV readability
            row = tuple('' if v is None else v for v in row)
            w.writerow(row)
            cnt += 1

    conn.close()
    print('WROTE', out, 'ROWS', cnt)


def main():
    p = argparse.ArgumentParser(description='Export WhatsApp message table to CSV')
    p.add_argument('--src', default='dump/Databases/msgstore.db.crypt15-decrypted/msgstore.db', help='path to sqlite msgstore.db')
    p.add_argument('--out', default='export/messages.csv', help='output CSV path')
    p.add_argument('--cols', default='_id,chat_row_id,from_me,key_id,timestamp,received_timestamp,message_type,text_data', help='comma separated columns to export')
    p.add_argument('--chat-id', type=int, default=None, help='filter by chat_row_id')
    p.add_argument('--order', choices=['asc','desc'], default='asc', help='order by timestamp')

    args = p.parse_args()
    cols = [c.strip() for c in args.cols.split(',') if c.strip()]
    export_messages(args.src, args.out, cols, args.chat_id, args.order)


if __name__ == '__main__':
    main()
