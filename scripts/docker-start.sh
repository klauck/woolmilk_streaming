#!/bin/bash

echo "Building WoolMilk Docker image..."

cd ../woolmilk

docker build -t woolmilk-streaming:latest .

echo "Starting WoolMilk Docker containers..."

docker-compose up