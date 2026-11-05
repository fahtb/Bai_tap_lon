import argparse
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from generator.make_reference import DATA
from producers.common import send

def records(run_id):
    with requests.Session() as session:
        session.mount('http://', HTTPAdapter(max_retries=Retry(total=4, backoff_factor=.5, status_forcelist=[429,500,502,503,504])))
        offset = 0
        while True:
            response = session.get(os.getenv('COUNTER_API_URL', 'http://counter-api:8000/sales'), params={'run_id':run_id,'offset':offset,'limit':500}, timeout=20)
            response.raise_for_status()
            page = response.json()['sales']
            if not page:
                break
            yield from page
            offset += len(page)

def run(run_id='demo',rate=None):
    if not run_id.replace('-', '').replace('_', '').isalnum():
        raise ValueError('Invalid run_id')
    path=DATA/'counter_drop'/f'{run_id}.csv'
    if not path.exists(): return {'sent':0,'already_processed':True}
    # The file is only a pending-batch marker. Payload is fetched through HTTP.
    result=send(records(run_id),'ticket_counter',rate)
    # Move only after every delivery is ACKed. A crash before move causes replay,
    # handled by stable event IDs at the sink, not by trusting filesystem state.
    processed=path.parent/'processed'; processed.mkdir(exist_ok=True)
    path.replace(processed/path.name)
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--run-id',default='demo'); p.add_argument('--rate',type=int); a=p.parse_args(); run(a.run_id,a.rate)
