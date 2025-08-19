#!/bin/bash
set -e

echo "🔥 Starting E-commerce Clickstream Data Producer..."

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Install requirements if needed
if [ ! -f "venv/installed" ]; then
    pip install -r requirements.txt
    touch venv/installed
fi

# Start the producer
python -m producer.producer