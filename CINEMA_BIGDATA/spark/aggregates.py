from pyspark.sql import functions as F, Window
from spark.storage import write_table

def latest_paid(events):
    priority=F.when(F.col('event_type')=='CANCELLED',3).when(F.col('event_type')=='PAID',2).otherwise(1)
    ordered=Window.partitionBy('run_id','ticket_id').orderBy(F.col('event_time').desc(),priority.desc(),F.col('event_id').desc())
    latest=events.withColumn('rank',F.row_number().over(ordered)).filter('rank=1').select('run_id','ticket_id',F.col('event_type').alias('latest_status'))
    # Revenue belongs to original payment hour even when cancellation arrives later.
    paid=events.filter("event_type='PAID'").withColumn('rank',F.row_number().over(ordered)).filter('rank=1').drop('rank')
    return paid.join(latest,['run_id','ticket_id']).withColumn('tickets',F.when(F.col('latest_status')=='PAID',1).otherwise(0)).withColumn('revenue',F.when(F.col('latest_status')=='PAID',F.col('price')).otherwise(0))

def refresh(spark):
    # A full ledger recomputation is deliberately simple and replay-safe for a
    # laptop demo. Its growing cost is a measured scaling limitation, not O(1).
    events=spark.read.format('org.apache.spark.sql.cassandra').options(keyspace='cinema',table='ticket_events').load()
    state=latest_paid(events).cache()
    for dimension,table in [('movie_id','revenue_by_movie_hour'),('channel','revenue_by_channel_hour')]:
        totals=state.groupBy(dimension,F.window('event_time','1 hour').alias('bucket')).agg(F.sum('tickets').alias('tickets'),F.sum('revenue').alias('revenue'))
        totals=totals.select(dimension,F.to_date('bucket.start').alias('date'),F.hour('bucket.start').alias('hour'),'tickets','revenue')
        write_table(totals,table)
    occupancy=state.groupBy('showtime_id').agg(F.sum('tickets').alias('sold_seats'),F.max('seat_count').alias('seat_count')).withColumn('occupancy_rate',F.col('sold_seats')/F.col('seat_count')).withColumn('updated_at',F.current_timestamp())
    write_table(occupancy,'occupancy_by_showtime')
    state.unpersist()
