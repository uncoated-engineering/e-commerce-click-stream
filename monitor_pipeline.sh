#!/bin/bash

echo "📊 E-commerce Clickstream Pipeline Status"
echo "========================================"

# Check Docker containers
echo "🐳 Docker Services Status:"
if command -v docker-compose &> /dev/null; then
    docker-compose ps
else
    echo "  ❌ Docker Compose not available"
fi

echo ""
echo "🌐 Service URLs:"
echo "  - Grafana Dashboard: http://localhost:3000"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - PostgreSQL: localhost:5432"

echo ""
echo "📈 Quick Stats (if PostgreSQL is running):"
if docker-compose exec postgres psql -U analytics_user -d ecommerce_analytics -c "SELECT 'Database connection successful'" 2>/dev/null; then
    docker-compose exec postgres psql -U analytics_user -d ecommerce_analytics -c "
    SELECT 
        'Total Events' as metric, COUNT(*)::text as value 
    FROM analytics.raw_events
    UNION ALL
    SELECT 
        'Active Sessions' as metric, COUNT(*)::text as value 
    FROM analytics.user_sessions
    UNION ALL
    SELECT 
        'Hourly Records' as metric, COUNT(*)::text as value 
    FROM analytics.hourly_metrics;
    " 2>/dev/null || echo "  📊 No data available yet"
else
    echo "  📊 Database not accessible or no data"
fi