"""Hàm thuần dùng trong UDF Spark và unit test; không gọi mạng."""
import math
from datetime import datetime, timedelta, timezone

def parse_money(value, counter=False):
    text=str(value)
    if counter: text=text.replace('.','')
    number=float(text)
    if not math.isfinite(number) or number<=0 or not number.is_integer():
        raise ValueError('BAD_PRICE')
    return int(number)

def parse_time(value, counter=False):
    if counter:
        parsed=datetime.strptime(value,'%d/%m/%Y %H:%M:%S').replace(tzinfo=timezone(timedelta(hours=7)))
    else:
        parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
        if parsed.tzinfo is None: raise ValueError('Timezone required')
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)

def normalize(record, source):
    counter=source=='ticket_counter'
    mapping={'run_id':'RunID','event_id':'EventID','ticket_id':'TicketID','event_type':'Type','showtime_id':'ShowID','seat':'SeatNo','price':'Amount','event_time':'TimeSold','payment_method':'PayType','customer_id':'Customer'}
    out={key:record.get(value if counter else key) for key,value in mapping.items()}
    out['channel']='counter' if counter else 'online'
    out['source']=source
    out['error_reason']=None
    for key in ['run_id','event_id','ticket_id','event_type','showtime_id','seat']:
        if not out.get(key): out['error_reason']='MISSING_FIELD'; break
    if out['event_type'] not in ['BOOKED','PAID','CANCELLED']:
        out['error_reason']=out['error_reason'] or 'BAD_EVENT_TYPE'
    try: out['price']=parse_money(out['price'],counter)
    except (ValueError,TypeError):
        out['price']=None; out['error_reason']=out['error_reason'] or 'BAD_PRICE'
    try: out['event_time']=parse_time(out['event_time'],counter)
    except (ValueError,TypeError,AttributeError):
        out['event_time']=None; out['error_reason']=out['error_reason'] or 'BAD_TIME'
    try: out['sent_at']=parse_time(record.get('sent_at',''))
    except (ValueError,TypeError,AttributeError): out['sent_at']=datetime.now(timezone.utc).replace(tzinfo=None)
    # Compare to ingress time carried with this receipt, stable on Spark retries.
    if out['event_time'] and out['event_time']>out['sent_at']+timedelta(minutes=5):
        out['error_reason']=out['error_reason'] or 'FUTURE'
    return out
