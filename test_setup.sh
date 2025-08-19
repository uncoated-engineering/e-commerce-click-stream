#!/bin/bash
set -e

echo "🧪 Testing E-commerce Clickstream Pipeline Setup..."

# Check if required files exist
echo "📁 Checking project structure..."
required_files=(
    "docker-compose.yml"
    ".env" 
    "requirements.txt"
    "producer/producer.py"
    "processor/streaming_processor.py"
    "db/init.sql"
    "start_infrastructure.sh"
    "start_producer.sh"
    "start_processor.sh"
)

for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✅ $file"
    else
        echo "  ❌ $file (missing)"
        exit 1
    fi
done

# Check if Docker is available
echo "🐳 Checking Docker..."
if command -v docker &> /dev/null && command -v docker-compose &> /dev/null; then
    echo "  ✅ Docker and Docker Compose are available"
else
    echo "  ❌ Docker or Docker Compose not found"
    exit 1
fi

# Check Python environment
echo "🐍 Checking Python environment..."
if [ -d "venv" ]; then
    echo "  ✅ Virtual environment exists"
    source venv/bin/activate
    if python --version | grep -q "3.1"; then
        echo "  ✅ Python 3.10+ detected"
    else
        echo "  ⚠️  Python version might be incompatible (3.10+ recommended)"
    fi
else
    echo "  ⚠️  Virtual environment not found. Create with: python -m venv venv"
fi

# Validate Docker Compose file
echo "🔧 Validating Docker Compose configuration..."
if docker-compose config > /dev/null 2>&1; then
    echo "  ✅ Docker Compose configuration is valid"
else
    echo "  ❌ Docker Compose configuration has errors"
    exit 1
fi

echo ""
echo "🎉 Setup validation completed successfully!"
echo ""
echo "Next steps:"
echo "1. Start infrastructure: ./start_infrastructure.sh"
echo "2. In another terminal, start producer: ./start_producer.sh"
echo "3. In another terminal, start processor: ./start_processor.sh"
echo "4. Access Grafana at http://localhost:3000 (admin/admin123)"