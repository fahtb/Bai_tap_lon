-- RF=1 is a single-node teaching deployment, not a high-availability claim.
CREATE KEYSPACE IF NOT EXISTS cinema WITH replication = {'class':'SimpleStrategy','replication_factor':1};
USE cinema;
CREATE TABLE IF NOT EXISTS revenue_by_movie_hour (movie_id text, date date, hour int, tickets bigint, revenue bigint, PRIMARY KEY ((movie_id,date),hour));
CREATE TABLE IF NOT EXISTS revenue_by_channel_hour (channel text, date date, hour int, tickets bigint, revenue bigint, PRIMARY KEY ((channel,date),hour));
CREATE TABLE IF NOT EXISTS occupancy_by_showtime (showtime_id text PRIMARY KEY, sold_seats bigint, seat_count int, occupancy_rate double, updated_at timestamp);
-- Deterministic ingested_at from Kafka timestamp makes retry use the same key.
CREATE TABLE IF NOT EXISTS invalid_events (date date, event_id text, source text, run_id text, error_reason text, raw_payload text, ingested_at timestamp, PRIMARY KEY ((date),ingested_at,event_id)) WITH default_time_to_live = 2592000;
CREATE TABLE IF NOT EXISTS seat_bookings (showtime_id text, seat text, event_id text, ticket_id text, PRIMARY KEY ((showtime_id),seat));
-- query_id avoids collisions between independent query batch counters.
CREATE TABLE IF NOT EXISTS pipeline_metrics (date date, query_id text, batch_id bigint, input_rows bigint, valid_rows bigint, invalid_rows bigint, duplicate_rows bigint, duration_ms double, input_rate double, processed_rate double, PRIMARY KEY ((date,query_id),batch_id));
CREATE TABLE IF NOT EXISTS daily_summary (date date, channel text, tickets bigint, revenue bigint, PRIMARY KEY ((date),channel));
-- Immutable ledger enables replay-safe absolute totals and independent verification.
CREATE TABLE IF NOT EXISTS ticket_events (run_id text, event_id text, ticket_id text, event_type text, showtime_id text, movie_id text, channel text, seat text, seat_count int, price bigint, event_time timestamp, sent_at timestamp, stored_at timestamp, source text, PRIMARY KEY ((run_id),event_id));
CREATE TABLE IF NOT EXISTS event_receipts (run_id text, source text, partition int, offset bigint, event_id text, error_reason text, PRIMARY KEY ((run_id),source,partition,offset));

