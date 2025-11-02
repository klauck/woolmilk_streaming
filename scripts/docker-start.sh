#!/bin/bash
set -e

# Ensure we have a local RSA key
if [ ! -f ~/.ssh/id_rsa.pub ]; then
  echo "No local RSA key found..."
  echo "Please add public key to (~/.ssh/id_rsa.pub)"
  # exit here because we need the key for SSH access to containers
  exit 1
fi

echo "Building WoolMilk Docker image..."

cd ../woolmilk

cp ~/.ssh/id_rsa.pub .

docker build -t woolmilk-streaming:latest .

rm id_rsa.pub

echo "Starting WoolMilk Docker containers..."
docker compose up -d

echo "Waiting for 5 seconds for containers to start..."
sleep 5

echo "Pre accepting host keys to avoid SSH prompt..."
ssh-keyscan -p 2201 localhost >> ~/.ssh/known_hosts 2>/dev/null || true
ssh-keyscan -p 2202 localhost >> ~/.ssh/known_hosts 2>/dev/null || true
ssh-keyscan -p 2203 localhost >> ~/.ssh/known_hosts 2>/dev/null || true
chmod 644 ~/.ssh/known_hosts

echo "Completed!"
echo "Run your cluster with:"
echo "python ../woolmilk/run_cluster.py --config docker-config-remote.json --mode remote"
