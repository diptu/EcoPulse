#!/bin/bash
set -e

echo "Starting Airflow in standalone mode..."
exec airflow standalone --port 8080 --host 0.0.0.0
