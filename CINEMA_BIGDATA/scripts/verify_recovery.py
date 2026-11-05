"""Read-only verification of the existing demo; never runs a producer.

The oracle reads the original JSON/CSV with pandas, independently of Spark's
normalization functions. Seat ordering here matches this fixture's single
micro-batch; it is not a general claim about ordering across Kafka partitions.
"""
import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import dict_factory


def stable(rows):
    return sorted([dict(row) for row in rows], key=lambda r: json.dumps(r, sort_keys=True, default=str))


def fingerprint(rows):
    return hashlib.sha256(json.dumps(stable(rows), sort_keys=True, default=str).encode()).hexdigest()


def oracle(data):
    online = pd.DataFrame(json.loads((data/'runs/demo/online.json').read_text()))
    counter = pd.read_csv(data/'runs/demo/counter.csv', dtype=str, keep_default_na=False).rename(columns={
        'RunID':'run_id','EventID':'event_id','Type':'event_type','TicketID':'ticket_id',
        'ShowID':'showtime_id','SeatNo':'seat','Amount':'price','TimeSold':'event_time'})
    online['event_time'] = pd.to_datetime(online['event_time'], format='ISO8601', errors='coerce', utc=True)
    counter['event_time'] = pd.to_datetime(counter['event_time'], format='%d/%m/%Y %H:%M:%S', errors='coerce').dt.tz_localize('Asia/Bangkok').dt.tz_convert('UTC')
    counter['price'] = counter['price'].str.replace('.','',regex=False)
    counter['channel'] = 'counter'
    frame = pd.concat([online,counter],ignore_index=True)
    raw_count = len(frame)
    frame = frame.drop_duplicates('event_id').copy()
    frame['price'] = pd.to_numeric(frame['price'],errors='coerce')
    reasons = {}
    for row in frame.to_dict('records'):
        reason = None
        if not row['showtime_id']: reason='MISSING_FIELD'
        elif pd.isna(row['price']) or row['price']<=0: reason='BAD_PRICE'
        elif pd.isna(row['event_time']): reason='BAD_TIME'
        elif row['event_time'] > pd.Timestamp.now(tz='UTC')+pd.Timedelta(minutes=5): reason='FUTURE'
        if reason: reasons[row['event_id']]=reason
    good=frame[~frame.event_id.isin(reasons)].copy()
    occupied, conflicts={},set()
    for row in good.sort_values(['event_time','event_id']).to_dict('records'):
        if row['event_type']!='PAID': continue
        key=(row['showtime_id'],row['seat'])
        if key in occupied and occupied[key]!=row['ticket_id']:
            conflicts.add(row['event_id']); reasons[row['event_id']]='DOUBLE_BOOKING'
        else: occupied[key]=row['ticket_id']
    good=good[~good.event_id.isin(conflicts)].copy()
    good['priority']=good.event_type.map({'BOOKED':1,'PAID':2,'CANCELLED':3})
    latest=good.sort_values(['event_time','priority','event_id']).drop_duplicates(['run_id','ticket_id'],keep='last')
    active=latest[latest.event_type=='PAID'].copy()
    shows=pd.read_csv(data/'reference/showtimes.csv')
    rooms=pd.read_csv(data/'reference/rooms.csv')
    enriched=good[good.event_type=='PAID'].merge(shows,on='showtime_id').merge(rooms,on='room_id')
    enriched['tickets']=enriched.ticket_id.isin(active.ticket_id).astype(int)
    enriched['revenue']=enriched.price*enriched.tickets
    enriched['date']=enriched.event_time.dt.strftime('%Y-%m-%d')
    enriched['hour']=enriched.event_time.dt.hour
    totals={}
    for dimension in ['movie_id','channel']:
        grouped=enriched.groupby([dimension,'date','hour'])[['tickets','revenue']].sum().reset_index()
        totals[dimension]={(r[dimension],r['date'],int(r['hour'])):(int(r['tickets']),int(r['revenue'])) for r in grouped.to_dict('records')}
    occupancy=enriched.groupby('showtime_id').agg(sold_seats=('tickets','sum'),seat_count=('seat_count','max'))
    return dict(raw=raw_count,duplicates=raw_count-len(frame),ledger_ids=set(good.event_id),invalid=reasons,
                revenue=int(enriched.revenue.sum()),tickets=int(enriched.tickets.sum()),totals=totals,occupancy=occupancy)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',required=True)
    parser.add_argument('--baseline')
    args=parser.parse_args()
    cluster=Cluster(['cassandra'],protocol_version=4)
    session=cluster.connect('cinema'); session.row_factory=dict_factory
    tables=['event_receipts','ticket_events','invalid_events','seat_bookings',
            'revenue_by_movie_hour','revenue_by_channel_hour','occupancy_by_showtime']
    try: contents={table:list(session.execute('SELECT * FROM '+table)) for table in tables}
    finally: cluster.shutdown()
    expected=oracle(Path('/data'))
    manifest=json.loads(Path('/data/runs/demo/fault_manifest.json').read_text())
    ledger=contents['ticket_events']; invalid=contents['invalid_events']; receipts=contents['event_receipts']
    actual_reasons={r['event_id']:r['error_reason'] for r in invalid}
    checks={
        'all_original_receipts': len(receipts)==expected['raw'],
        'duplicate_receipts_match_manifest': len(receipts)-len({r['event_id'] for r in receipts})==expected['duplicates']==manifest['injected']['DUPLICATE'],
        'exact_accepted_event_ids': {r['event_id'] for r in ledger}==expected['ledger_ids'],
        'exact_invalid_ids_and_reasons': actual_reasons==expected['invalid'],
        'unique_ledger_ids': len(ledger)==len({r['event_id'] for r in ledger}),
    }
    for dimension,table in [('movie_id','revenue_by_movie_hour'),('channel','revenue_by_channel_hour')]:
        actual={(r[dimension],str(r['date']),r['hour']):(r['tickets'],r['revenue']) for r in contents[table]}
        checks['exact_'+table]=actual==expected['totals'][dimension]
    occ=expected['occupancy']
    checks['exact_occupancy']=len(contents['occupancy_by_showtime'])==len(occ) and all(
        r['showtime_id'] in occ.index and r['sold_seats']==int(occ.loc[r['showtime_id'],'sold_seats'])
        and r['seat_count']==int(occ.loc[r['showtime_id'],'seat_count'])
        and abs(r['occupancy_rate']-r['sold_seats']/r['seat_count'])<1e-10
        for r in contents['occupancy_by_showtime'])
    snapshot={
        'checked_at':datetime.now(timezone.utc).isoformat(),
        'counts':{table:len(rows) for table,rows in contents.items()},
        'hashes':{table:fingerprint(rows) for table,rows in contents.items()},
        'expected_revenue_vnd':expected['revenue'],'expected_sold_tickets':expected['tickets'],
        'actual_revenue_vnd':sum(r['revenue'] for r in contents['revenue_by_channel_hour']),
        'invalid_reasons':dict(Counter(actual_reasons.values())),
        'checkpoint_commits':{q:sorted(int(p.name) for p in (Path('/checkpoints')/q/'commits').glob('*') if p.name.isdigit()) for q in ['audit','valid']},
        'checks':checks,
    }
    if args.baseline:
        before=json.loads(Path(args.baseline).read_text())
        checks['all_sink_rows_unchanged_after_recovery']=snapshot['hashes']==before['hashes']
        checks['all_counts_unchanged_after_recovery']=snapshot['counts']==before['counts']
    Path(args.output).write_text(json.dumps(snapshot,indent=2,default=str))
    print(json.dumps(snapshot,indent=2,default=str))
    if not all(checks.values()): raise SystemExit(1)

if __name__=='__main__': main()
