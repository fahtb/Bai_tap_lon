from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

ONLINE_FIELDS = ['run_id','event_id','event_type','ticket_id','showtime_id','seat','price','currency','payment_method','customer_id','event_time','channel','sent_at']
COUNTER_FIELDS = ['RunID','EventID','Type','TicketID','ShowID','SeatNo','Amount','TimeSold','Cashier','PayType','Customer','sent_at']
ONLINE_SCHEMA = StructType([StructField(x,StringType(),True) for x in ONLINE_FIELDS])
COUNTER_SCHEMA = StructType([StructField(x,StringType(),True) for x in COUNTER_FIELDS])
NORMAL_SCHEMA = StructType([
    StructField(x, LongType() if x=='price' else TimestampType() if x in ['event_time','sent_at'] else StringType(),True)
    for x in ['run_id','event_id','ticket_id','event_type','showtime_id','seat','price','event_time','payment_method','customer_id','channel','source','error_reason','sent_at']
])
