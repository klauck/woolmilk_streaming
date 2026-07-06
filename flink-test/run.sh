#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "==> resetting results/"
rm -rf results/sink_1 results/sink_2

echo "==> building + starting Flink cluster"
docker compose up -d --build

echo "==> waiting for JobManager REST (localhost:8081)"
for _ in $(seq 1 60); do
  if curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
    echo "    JobManager up"
    break
  fi
  sleep 2
done

echo "==> submitting job"
docker compose exec -T jobmanager flink run -py /opt/flink-test/job.py

echo "==> done. results:"
echo "    results/sink_1/  results/sink_2/"
ls -R results || true
