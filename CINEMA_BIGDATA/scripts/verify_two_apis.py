"""Real HTTP integration test; intercept only Kafka send and marker movement."""
import csv
import json
from pathlib import Path
from unittest.mock import patch
import requests
from producers import producer_online, producer_counter

def main():
    expected_online = json.loads(Path('/data/runs/demo/online.json').read_text())
    with Path('/data/runs/demo/counter.csv').open(newline='', encoding='utf-8') as f:
        expected_counter = list(csv.DictReader(f))
    captured = {}
    def capture(records, topic, rate):
        captured[topic] = list(records)
        return {'sent':len(captured[topic]), 'test_only':True}
    with patch.object(producer_online, 'send', capture):
        producer_online.run('demo')
    # Emulate an eligible batch without re-enqueuing or moving any actual file.
    with patch.object(producer_counter, 'send', capture), patch.object(Path, 'exists', return_value=True), patch.object(Path, 'mkdir'), patch.object(Path, 'replace'):
        producer_counter.run('demo')
    assert captured['ticket_online'] == expected_online
    assert captured['ticket_counter'] == expected_counter
    assert producer_counter.run('demo')['already_processed'] is True
    for url in ['http://generator-api:8000/events','http://counter-api:8000/sales']:
        assert requests.get(url, params={'offset':-1}, timeout=10).status_code == 400
        assert requests.get(url, params={'run_id':'does-not-exist'}, timeout=10).status_code == 404
    print(json.dumps({'online_http_records':len(expected_online), 'counter_http_records':len(expected_counter),
                      'payloads_match_originals':True, 'pagination_and_error_checks':True,
                      'processed_demo_skipped':True, 'kafka_messages_published':0,
                      'method':'Real HTTP; Kafka send intercepted for no-replay test'}, indent=2))

if __name__ == '__main__':
    main()
