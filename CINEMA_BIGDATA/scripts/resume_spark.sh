#!/bin/bash
# Resume existing containers only; never re-run generators or producers.
set -euo pipefail
cd "$(dirname "$0")/.."
for service in kafka cassandra; do
  id=$(docker compose ps -aq "$service")
  test -n "$id" || { echo "Missing container: $service; stop to avoid creating a new dataset."; exit 1; }
  docker start "$id" >/dev/null
  ready=false
  for attempt in $(seq 1 60); do
    if [ "$(docker inspect --format '{{.State.Health.Status}}' "$id")" = healthy ]; then
      ready=true
      break
    fi
    sleep 3
  done
  "$ready" || { echo "$service not healthy; inspect its logs."; exit 1; }
done
id=$(docker compose ps -aq spark-streaming)
test -n "$id" || { echo 'Missing Spark container; no new container will be created.'; exit 1; }
docker start "$id"
docker compose ps -a
