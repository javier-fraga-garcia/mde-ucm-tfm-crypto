#!/bin/bash
echo "Creating Kafka topics..."
echo "Creating kafka-envelope topic"
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic kafka-envelope --partitions 3
echo "Listing topics"
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
echo "Topics created successfully!"