#!/bin/bash
echo "building source node..."
docker build -t woolmilk-source:latest ./source-node

echo "building processing node..."
docker build -t woolmilk-processing:latest ./processing-node

echo "building sink node..."
docker build -t woolmilk-sink:latest ./sink-node

echo "All images built successfully."