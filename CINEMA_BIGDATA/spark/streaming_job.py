"""One continuous Spark process, isolated checkpoints per query.

Raw audit and validation use separate sinks. Stateful deduplication only sees
valid event times: malformed/future timestamps must not advance the watermark.
"""
import json
import os
import time
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.streaming import StreamingQueryListener
from producers.normalization import normalize
from spark.schemas import ONLINE_SCHEMA, COUNTER_SCHEMA, NORMAL_SCHEMA
from spark.storage import connect, write_invalid, reserve_seat, write_ledger
from spark.aggregates import refresh

spark=(SparkSession.builder.appName('CinemaStreaming').config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','3').config('spark.cassandra.connection.host','cassandra').config('spark.cassandra.output.concurrent.writes','4').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
METRICS=Path('/data/metrics'); METRICS.mkdir(exist_ok=True,parents=True)

class Progress(StreamingQueryListener):
    def onQueryStarted(self,event):
        # The query ID is checkpoint-stable; runId changes on each restart.
        print(json.dumps({'event':'query_started','query':event.name,'id':str(event.id),'run_id':str(event.runId)}),flush=True)
    def onQueryTerminated(self,event):
        print(json.dumps({'event':'query_terminated','id':str(event.id),'exception':event.exception}),flush=True)
    def onQueryIdle(self,event): pass
    def onQueryProgress(self,event):
        progress=json.loads(event.progress.json)
        with (METRICS/'progress.jsonl').open('a') as f: f.write(json.dumps(progress)+'\n')

spark.streams.addListener(Progress())
raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers','kafka:29092').option('subscribe','ticket_online,ticket_counter').option('startingOffsets','earliest').option('maxOffsetsPerTrigger',os.getenv('MAX_OFFSETS_PER_TRIGGER','3000')).load())

@F.udf(NORMAL_SCHEMA)
def canonical(row,topic): return normalize(row.asDict() if row else {},topic)

def parse_branch(topic,schema):
    return (raw.filter(F.col('topic')==topic).withColumn('raw_payload',F.col('value').cast('string')).withColumn('decoded',F.from_json('raw_payload',schema)).withColumn('normalized',canonical('decoded','topic')).select('normalized.*','raw_payload',F.col('timestamp').alias('kafka_time'),'partition','offset'))

parsed=parse_branch('ticket_online',ONLINE_SCHEMA).unionByName(parse_branch('ticket_counter',COUNTER_SCHEMA))
shows=spark.read.schema('showtime_id STRING,movie_id STRING,room_id STRING,start_time STRING,base_price LONG').option('header',True).csv('/data/reference/showtimes.csv')
rooms=spark.read.schema('room_id STRING,cinema_id STRING,seat_count INT').option('header',True).csv('/data/reference/rooms.csv')
movies=spark.read.schema('movie_id STRING,title STRING,genre STRING,duration_min INT,release_date STRING').option('header',True).csv('/data/reference/movies.csv')
reference=shows.join(rooms,'room_id').join(movies,'movie_id').select('showtime_id','movie_id','seat_count')
enriched=parsed.join(F.broadcast(reference),'showtime_id','left').withColumn('error_reason',F.when(F.col('error_reason').isNotNull(),F.col('error_reason')).when(F.col('movie_id').isNull(),'UNKNOWN_SHOWTIME'))

def audit(batch,batch_id):
    started=time.monotonic(); cluster,session=connect()
    counts={'input':0,'invalid':0,'valid':0}
    try:
        for row in batch.toLocalIterator():
            r=row.asDict(); counts['input']+=1
            # Kafka coordinates are immutable receipt IDs; audit survives retries.
            session.execute('INSERT INTO event_receipts (run_id,source,partition,offset,event_id,error_reason) VALUES (%s,%s,%s,%s,%s,%s)',(r['run_id'] or 'unknown',r['source'],r['partition'],r['offset'],r['event_id'] or f"{r['partition']}-{r['offset']}",r['error_reason']))
            if r['error_reason']:
                write_invalid(session,r); counts['invalid']+=1
            else: counts['valid']+=1
        session.execute('INSERT INTO pipeline_metrics (date,query_id,batch_id,input_rows,valid_rows,invalid_rows,duration_ms) VALUES (%s,%s,%s,%s,%s,%s,%s)',(datetime.utcnow().date(),'audit',batch_id,counts['input'],counts['valid'],counts['invalid'],(time.monotonic()-started)*1000))
    finally: cluster.shutdown()

def valid_sink(batch,batch_id):
    started=time.monotonic(); cluster,session=connect(); count=0
    try:
        # Ordering only defines ties inside a micro-batch. LWT is authoritative
        # across batches and processes, so replay never rejects the same owner.
        for row in batch.orderBy('event_time','event_id').toLocalIterator():
            r=row.asDict()
            if reserve_seat(session,r): write_ledger(session,r); count+=1
            else: write_invalid(session,r,'DOUBLE_BOOKING')
        if count: refresh(spark)
        session.execute('INSERT INTO pipeline_metrics (date,query_id,batch_id,valid_rows,duration_ms) VALUES (%s,%s,%s,%s,%s)',(datetime.utcnow().date(),'valid',batch_id,count,(time.monotonic()-started)*1000))
        print(json.dumps({'query':'valid','batch_id':batch_id,'written':count}),flush=True)
    finally: cluster.shutdown()

audit_query=(enriched.writeStream.queryName('audit').foreachBatch(audit).outputMode('append').option('checkpointLocation','/checkpoints/audit').trigger(processingTime='10 seconds').start())
# dropDuplicates(event_id) alone cannot evict an event-time-free key reliably.
# 3.5's WithinWatermark implements the intended bounded state. Too-late events
# are observable via progress stateOperators.numRowsDroppedByWatermark.
valid=enriched.filter('error_reason IS NULL').withWatermark('event_time','10 minutes').dropDuplicatesWithinWatermark(['event_id'])
valid_query=(valid.writeStream.queryName('valid').foreachBatch(valid_sink).outputMode('update').option('checkpointLocation','/checkpoints/valid').trigger(processingTime='10 seconds').start())
spark.streams.awaitAnyTermination()
