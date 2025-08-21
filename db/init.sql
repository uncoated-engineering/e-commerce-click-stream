-- Drop existing schema to start fresh
DROP SCHEMA IF EXISTS analytics CASCADE;

-- Create analytics schema
CREATE SCHEMA analytics;

-- Raw events table with proper timestamp types
CREATE TABLE analytics.raw_events (
    event_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    product_id TEXT,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    session_id TEXT NOT NULL,
    page_url VARCHAR(500),
    user_agent VARCHAR(500),
    ip_address INET,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- User session metrics with proper timestamp types
CREATE TABLE analytics.user_sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    session_duration_seconds INTEGER DEFAULT 0,
    page_views INTEGER DEFAULT 0,
    add_to_cart_events INTEGER DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    total_purchase_amount DECIMAL(10,2) DEFAULT 0,
    converted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Product metrics
CREATE TABLE analytics.product_metrics (
    product_id TEXT PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(100),
    total_views INTEGER DEFAULT 0,
    total_cart_adds INTEGER DEFAULT 0,
    total_purchases INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5,2) DEFAULT 0,
    revenue DECIMAL(12,2) DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Hourly aggregated metrics
CREATE TABLE analytics.hourly_metrics (
    hour_timestamp TIMESTAMP WITH TIME ZONE PRIMARY KEY,
    total_events INTEGER DEFAULT 0,
    unique_users INTEGER DEFAULT 0,
    page_views INTEGER DEFAULT 0,
    cart_additions INTEGER DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5,2) DEFAULT 0,
    revenue DECIMAL(12,2) DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Real-time dashboard metrics
CREATE TABLE analytics.dashboard_metrics (
    metric_key VARCHAR(100) PRIMARY KEY,
    metric_value DECIMAL(15,4),
    metric_label VARCHAR(200),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_raw_events_timestamp ON analytics.raw_events(timestamp);
CREATE INDEX idx_raw_events_user_id ON analytics.raw_events(user_id);
CREATE INDEX idx_raw_events_session_id ON analytics.raw_events(session_id);
CREATE INDEX idx_raw_events_event_type ON analytics.raw_events(event_type);
CREATE INDEX idx_user_sessions_user_id ON analytics.user_sessions(user_id);
CREATE INDEX idx_user_sessions_start_time ON analytics.user_sessions(start_time);
CREATE INDEX idx_hourly_metrics_timestamp ON analytics.hourly_metrics(hour_timestamp);

-- Insert initial dashboard metrics
INSERT INTO analytics.dashboard_metrics (metric_key, metric_value, metric_label) VALUES
('total_users', 0, 'Total Active Users'),
('total_sessions', 0, 'Total Sessions'),
('conversion_rate', 0, 'Overall Conversion Rate (%)'),
('avg_session_duration', 0, 'Average Session Duration (seconds)'),
('total_revenue', 0, 'Total Revenue ($)')
ON CONFLICT (metric_key) DO NOTHING;

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION analytics.update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for user_sessions
CREATE TRIGGER update_user_sessions_updated_at
    BEFORE UPDATE ON analytics.user_sessions
    FOR EACH ROW
    EXECUTE FUNCTION analytics.update_updated_at();