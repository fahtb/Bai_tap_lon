"""One small storage module; functions correspond to visible Cassandra tables."""
import hashlib
import json
import os
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

def connect():
    cluster=Cluster([os.getenv('CASSANDRA_HOST','cassandra')],protocol_version=4)
    session=cluster.connect('cinema')
    session.row_factory=dict_factory
    session.default_timeout=60
    return cluster, session

def write_invalid(session, record, reason=None):
    # A fixed deterministic timestamp avoids a new primary key on every retry.
    event_id=record.get('event_id') or hashlib.sha256(record['raw_payload'].encode()).hexdigest()
    day=(record.get('sent_at') or record['kafka_time']).date()
    stamp=datetime.combine(day,datetime.min.time())+timedelta(milliseconds=int(hashlib.sha256(event_id.encode()).hexdigest()[:12],16)%86400000)
    session.execute('INSERT INTO invalid_events (date,ingested_at,event_id,source,error_reason,raw_payload,run_id) VALUES (%s,%s,%s,%s,%s,%s,%s)',(day,stamp,event_id,record['source'],reason or record['error_reason'],record['raw_payload'],record.get('run_id','unknown')))

def reserve_seat(session, record):
    if record['event_type']!='PAID': return True
    # LWT serializes competing tickets for this show/seat. The existing owner is
    # accepted on replay; [applied]=false alone does NOT imply double booking.
    row=session.execute('INSERT INTO seat_bookings (showtime_id,seat,event_id,ticket_id) VALUES (%s,%s,%s,%s) IF NOT EXISTS',(record['showtime_id'],record['seat'],record['event_id'],record['ticket_id'])).one()
    return row['[applied]'] or row.get('ticket_id')==record['ticket_id']

def write_ledger(session, record):
    fields=['run_id','event_id','ticket_id','event_type','showtime_id','movie_id','channel','seat','seat_count','price','event_time','sent_at','stored_at','source']
    values=[record.get(x) for x in fields]
    values[fields.index('stored_at')]=datetime.utcnow()
    # Keep first write time for actual end-to-end latency; do not change on retry.
    statement='INSERT INTO ticket_events ('+','.join(fields)+') VALUES ('+','.join(['%s']*len(fields))+') IF NOT EXISTS'
    session.execute(statement,values)

def write_table(df, table):
    # Write absolute values rather than counters: re-running a micro-batch is safe.
    df.write.format('org.apache.spark.sql.cassandra').options(keyspace='cinema',table=table).mode('append').save()
