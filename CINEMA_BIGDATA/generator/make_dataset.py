"""Fixtures rời nhau theo run_id; giữ raw để oracle độc lập đối chiếu."""
import argparse
import csv
import json
import os
import random
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from generator.make_reference import DATA

TZ = timezone(timedelta(hours=7))

def make_dataset(run_id='demo', total=None, rate=None, seed=None):
    total = total or int(os.getenv('TOTAL_EVENTS','1000'))
    rate = rate or int(os.getenv('EVENTS_PER_SEC','100'))
    rng = random.Random(seed if seed is not None else int(os.getenv('SEED','42')))
    base = datetime.fromisoformat(os.getenv('BASE_TIME','2026-10-25T07:00:00+00:00'))
    shows = list(csv.DictReader((DATA/'reference/showtimes.csv').open()))
    hot = [s for s in shows if int(s['movie_id'][1:]) < 5]
    records, faults = [], Counter()
    used_seats = Counter()
    # Mutually exclusive faults make manifest counts unambiguous.
    fault_types = [('MISSING',float(os.getenv('MISSING_RATE','.02'))),('BAD_PRICE',float(os.getenv('BAD_PRICE_RATE','.01'))),('BAD_TIME',float(os.getenv('BAD_TIME_RATE','.01'))),('LATE',float(os.getenv('LATE_RATE','.02'))),('FUTURE',float(os.getenv('FUTURE_RATE','.005'))),('DOUBLE_BOOKING',float(os.getenv('DOUBLE_BOOKING_RATE','.005')))]
    labels = []
    for kind, fraction in fault_types:
        labels += [kind]*int(total*fraction)
    labels += ['OK'] * max(0,total-len(labels))
    rng.shuffle(labels)
    prior_paid = None
    for i in range(total):
        show = rng.choice(hot if rng.random()<.6 else shows)
        # At most one normal PAID per seat; rare full rooms select another show.
        while used_seats[show['showtime_id']] >= 80:
            show = rng.choice(shows)
        used_seats[show['showtime_id']] += 1
        seat_number = used_seats[show['showtime_id']]
        event_time = base + timedelta(seconds=i/rate)
        event = dict(run_id=run_id,event_id=str(uuid.uuid5(uuid.NAMESPACE_URL,f'{run_id}/{i}')),event_type='PAID',ticket_id=f'{run_id}-T{i:09}',showtime_id=show['showtime_id'],seat=f'{chr(65+(seat_number-1)//10)}{(seat_number-1)%10+1:02}',price=int(show['base_price']),currency='VND',payment_method=rng.choice(['momo','card','bank']),customer_id=f'U{i%10000:06}',event_time=event_time.isoformat(),channel='online' if i%2==0 else 'counter')
        kind = labels[i]
        if kind == 'MISSING': event['showtime_id'] = ''
        elif kind == 'BAD_PRICE': event['price'] = -95000 if i%2 else 'not-a-number'
        elif kind == 'BAD_TIME': event['event_time'] = 'not-a-time'
        elif kind == 'LATE': event['event_time'] = (event_time-timedelta(minutes=rng.randint(1,9))).isoformat()
        elif kind == 'FUTURE': event['event_time'] = (base+timedelta(days=3650)).isoformat()
        elif kind == 'DOUBLE_BOOKING':
            if prior_paid:
                event.update(showtime_id=prior_paid['showtime_id'],seat=prior_paid['seat'])
            else: kind = 'OK'
        if kind != 'OK': faults[kind] += 1
        if kind == 'OK': prior_paid = event.copy()
        records.append(event)
    # Lifecycle events have their own IDs, same ticket_id and seat.
    valid = [e for e in records if e['showtime_id'] and isinstance(e['price'],int) and e['price']>0 and e['event_time']!='not-a-time' and e['event_time']< (base+timedelta(days=1)).isoformat()]
    for i,e in enumerate(valid[:max(1,total//50)]):
        booked = dict(e,event_id=str(uuid.uuid5(uuid.NAMESPACE_URL,e['event_id']+'/book')),event_type='BOOKED',event_time=(datetime.fromisoformat(e['event_time'])-timedelta(seconds=10)).isoformat())
        records.append(booked)
        if i%2==0:
            records.append(dict(e,event_id=str(uuid.uuid5(uuid.NAMESPACE_URL,e['event_id']+'/cancel')),event_type='CANCELLED',event_time=(datetime.fromisoformat(e['event_time'])+timedelta(seconds=10)).isoformat()))
    for e in rng.sample(records, int(total*float(os.getenv('DUPLICATE_RATE','.03')))):
        records.append(dict(e))
        faults['DUPLICATE'] += 1
    folder = DATA/'runs'/run_id
    folder.mkdir(parents=True,exist_ok=True)
    online, counter = [], []
    for e in records:
        if e['channel']=='online': online.append(e)
        else:
            try: time_sold=datetime.fromisoformat(e['event_time']).astimezone(TZ).strftime('%d/%m/%Y %H:%M:%S')
            except ValueError: time_sold=e['event_time']
            amount=f"{e['price']:,}".replace(',','.') if isinstance(e['price'],int) else e['price']
            counter.append(dict(RunID=run_id,EventID=e['event_id'],Type=e['event_type'],TicketID=e['ticket_id'],ShowID=e['showtime_id'],SeatNo=e['seat'],Amount=amount,TimeSold=time_sold,Cashier='POS01',PayType='cash',Customer=e['customer_id']))
    (folder/'online.json').write_text(json.dumps(online),encoding='utf-8')
    for location in [folder/'counter.csv',DATA/'counter_drop'/f'{run_id}.csv']:
        location.parent.mkdir(parents=True,exist_ok=True)
        with location.open('w',newline='',encoding='utf-8') as f:
            w=csv.DictWriter(f,fieldnames=list(counter[0])); w.writeheader(); w.writerows(counter)
    manifest=dict(run_id=run_id,seed=seed or int(os.getenv('SEED','42')),base_time=base.isoformat(),base_events=total,total_records=len(records),online=len(online),counter=len(counter),injected=dict(faults))
    (folder/'fault_manifest.json').write_text(json.dumps(manifest,indent=2))
    (DATA/'fault_manifest.json').write_text(json.dumps(manifest,indent=2))
    print(json.dumps(manifest))
    return manifest

if __name__=='__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--run-id',default='demo'); parser.add_argument('--total',type=int); parser.add_argument('--rate',type=int)
    args=parser.parse_args(); make_dataset(args.run_id,args.total,args.rate)
