import argparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from producers.common import send

def run(run_id='demo',rate=None):
    session=requests.Session()
    session.mount('http://',HTTPAdapter(max_retries=Retry(total=4,backoff_factor=.5,status_forcelist=[429,500,502,503,504])))
    def records():
        offset=0
        while True:
            response=session.get('http://generator-api:8000/events',params=dict(run_id=run_id,offset=offset,limit=500),timeout=20)
            response.raise_for_status(); page=response.json()['events']
            if not page: break
            yield from page
            offset+=len(page)
    return send(records(),'ticket_online',rate)

if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--run-id',default='demo'); p.add_argument('--rate',type=int); a=p.parse_args(); run(a.run_id,a.rate)
