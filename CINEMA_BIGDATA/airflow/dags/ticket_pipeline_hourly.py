"""Counter HTTP API ingestion; Spark stays continuous."""
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

def alert(context):
    logging.error('CINEMA_ALERT task=%s run=%s error=%s', context['task_instance'].task_id,
                  context['run_id'], context.get('exception'))

def check_kafka():
    from confluent_kafka.admin import AdminClient
    metadata = AdminClient({'bootstrap.servers': 'kafka:29092'}).list_topics(timeout=15)
    for name in ['ticket_online', 'ticket_counter']:
        assert name in metadata.topics and metadata.topics[name].error is None, name
    return {'topics': ['ticket_online', 'ticket_counter']}

def check_cassandra():
    from spark.storage import connect
    cluster, session = connect()
    try:
        return session.execute("SELECT count(*) AS n FROM event_receipts WHERE run_id='demo'").one()['n']
    finally:
        cluster.shutdown()

def run_counter():
    from producers.producer_counter import run
    return run('demo')

def check_source_apis():
    import requests
    result = {}
    for source, url, key in [('online','http://generator-api:8000/events','events'),
                             ('counter','http://counter-api:8000/sales','sales')]:
        response = requests.get(url, params={'run_id':'demo','limit':1}, timeout=20)
        response.raise_for_status()
        payload = response.json()
        assert isinstance(payload[key], list)
        result[source] = {'http_status':response.status_code, 'total':payload['total']}
    return result

def verify(ti):
    import time
    sent = ti.xcom_pull(task_ids='run_counter_batch')['sent']
    before = ti.xcom_pull(task_ids='check_cassandra')
    for _ in range(30):
        current = check_cassandra()
        if current >= before + sent:
            return {'receipts': current, 'new_messages': sent,
                    'note': 'No pending counter API batch; no replay' if sent == 0 else 'Spark receipts observed'}
        time.sleep(5)
    raise RuntimeError('Spark has not recorded the new counter messages')

def refresh_customers():
    from scripts.customer_views import refresh
    return refresh('demo')

with DAG('ticket_pipeline_hourly', start_date=datetime(2026, 10, 25), schedule='@hourly',
         catchup=False, max_active_runs=1, tags=['cinema'],
         default_args={'retries': 2, 'retry_delay': timedelta(seconds=30),
                       'execution_timeout': timedelta(minutes=5), 'on_failure_callback': alert},
         doc_md='API online /events + API quầy vé /sales → Kafka → Spark → Cassandra. DAG gọi kiểm tra cả hai API, gửi đợt quầy vé chưa xử lý qua HTTP và kiểm tra tiếp nhận. Spark chạy liên tục bên ngoài DAG. Đợt demo đã gửi thì gửi 0 bản tin.') as dag:
    apis = PythonOperator(task_id='check_source_apis', python_callable=check_source_apis)
    kafka = PythonOperator(task_id='check_kafka', python_callable=check_kafka)
    cassandra = PythonOperator(task_id='check_cassandra', python_callable=check_cassandra)
    counter = PythonOperator(task_id='run_counter_batch', python_callable=run_counter)
    verified = PythonOperator(task_id='verify_new_data', python_callable=verify)
    customers = PythonOperator(task_id='refresh_customer_views', python_callable=refresh_customers)
    apis >> kafka >> cassandra >> counter >> verified >> customers
