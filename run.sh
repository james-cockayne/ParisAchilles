#!/bin/bash

# SQL Converter Docker Build and Run Script
# This script builds and runs the SQL Server to DuckDB converter

echo "Building Docker image..."
docker build -t sql-converter:latest .

if [ $? -eq 0 ]; then
    echo "Build successful!"
    echo ""
    echo "Running SQL converter..."
    echo "================================"
    docker run --rm -v "C:\Code\test:/app/data" -e DATABASE_NAME=database.db sql-converter:latest
else
    echo "Build failed!"
    exit 1
fi
