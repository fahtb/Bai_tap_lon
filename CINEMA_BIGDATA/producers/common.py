import json
import os
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

def send(records, topic, rate=None):
    rate = rate or int(os.getenv('EVENTS_PER_SEC','100'))
    # librdkafka supports idempotence; delivery callback distinguishes enqueue from ACK.
    producer=Producer({'bootstrap.servers':os.getenv('KAFKA_BOOTSTRAP','kafka:29092'),'acks':'all','enable.idempotence':True,'retries':10,'retry.backoff.ms':200,'delivery.timeout.ms':120000})
    counts={'sent':0,'failed':0}; start=time.monotonic()
    def delivered(error, message):
        counts['failed' if error else 'sent']+=1
        if error: print(str(error),flush=True)
    for i,record in enumerate(records):
        payload=dict(record,sent_at=datetime.now(timezone.utc).isoformat())
        key=record.get('showtime_id',record.get('ShowID','')) or 'invalid'
        while True:
            try:
                producer.produce(topic,key=key,value=json.dumps(payload),on_delivery=delivered); break
            except BufferError: producer.poll(.1)
        producer.poll(0)
        delay=(i+1)/rate-(time.monotonic()-start)
        if delay>0: time.sleep(delay)
    remaining=producer.flush(120)
    counts['duration_seconds']=time.monotonic()-start
    print(json.dumps(counts),flush=True)
    if remaining or counts['failed']: raise RuntimeError(f'Kafka delivery incomplete: {counts}, pending={remaining}')
    return counts
