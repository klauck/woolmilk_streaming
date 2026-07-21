#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "==> resetting results/"
rm -rf results && mkdir -p results

echo "==> building image"
docker build -t woolmilk-flink-ds:1.20.0 .

echo "==> starting cluster (3 taskmanagers via deploy.replicas)"
docker compose up -d

echo "==> waiting for JobManager REST (localhost:8081)"
for _ in $(seq 1 60); do
  if curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
    echo "    JobManager up"
    break
  fi
  sleep 2
done

echo "==> submitting job_generator.py"
docker compose exec -T jobmanager flink run -py /opt/flink-datastream/job_generator.py

echo "==> results:"
ls -R results || true
