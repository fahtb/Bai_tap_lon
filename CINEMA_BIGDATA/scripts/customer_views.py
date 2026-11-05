"""Enrich accepted ledger events using retained raw input; never publish Kafka.

Profiles are explicitly synthetic. Stable primary keys make refresh repeatable.
This is a presentation projection, not a second streaming pipeline.
"""
import csv
import json
import os
from pathlib import Path
from spark.storage import connect

def refresh(run_id='demo'):
    folder = Path(os.getenv('DATA_DIR', '/data')) / 'runs' / run_id
    online = json.loads((folder / 'online.json').read_text(encoding='utf-8'))
    with (folder / 'counter.csv').open(encoding='utf-8', newline='') as f:
        counter = list(csv.DictReader(f))
    raw = {e['event_id']: (e.get('customer_id'), e.get('payment_method')) for e in online}
    raw.update({e['EventID']: (e.get('Customer'), e.get('PayType')) for e in counter})
    cluster, session = connect()
    try:
        session.execute('CREATE TABLE IF NOT EXISTS customers (customer_id text PRIMARY KEY, full_name text, email text, city text, is_synthetic boolean)')
        session.execute('CREATE TABLE IF NOT EXISTS customer_events (customer_id text, run_id text, event_time timestamp, event_id text, ticket_id text, event_type text, showtime_id text, movie_id text, seat text, channel text, price bigint, payment_method text, PRIMARY KEY ((customer_id,run_id),event_time,event_id)) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC)')
        rows = list(session.execute('SELECT * FROM ticket_events WHERE run_id=%s', (run_id,)))
        missing = [r['event_id'] for r in rows if not raw.get(r['event_id'], (None,))[0]]
        if missing:
            raise ValueError('Accepted events without customer mapping: ' + str(len(missing)))
        customer_insert = session.prepare('INSERT INTO customers (customer_id,full_name,email,city,is_synthetic) VALUES (?,?,?,?,?)')
        event_insert = session.prepare('INSERT INTO customer_events (customer_id,run_id,event_time,event_id,ticket_id,event_type,showtime_id,movie_id,seat,channel,price,payment_method) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)')
        ids = set()
        for row in rows:
            customer, payment = raw[row['event_id']]
            if customer not in ids:
                # A clearly labelled fictional profile, never a real identity.
                index = int(customer[1:])
                name = ['Nguyen Minh An', 'Tran Bao Chau', 'Le Hoang Nam', 'Pham Ngoc Linh'][index % 4]
                session.execute(customer_insert, (customer, name + ' ' + customer, customer.lower() + '@example.com', ['HCM', 'Hanoi', 'Danang'][index % 3], True))
                ids.add(customer)
            session.execute(event_insert, (customer, run_id, row['event_time'], row['event_id'], row['ticket_id'], row['event_type'], row['showtime_id'], row['movie_id'], row['seat'], row['channel'], row['price'], payment))
        result = {'run_id': run_id, 'customers': len(ids), 'accepted_events_projected': len(rows), 'synthetic_profiles': True, 'kafka_messages_published': 0}
        print(json.dumps(result))
        return result
    finally:
        cluster.shutdown()

if __name__ == '__main__':
    refresh()
