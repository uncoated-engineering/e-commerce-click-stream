-- Create analytics schema
CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw events table (for backup/audit)
CREATE TABLE IF NOT EXISTS analytics.raw_events (
    event_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    product_id UUID,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    session_id UUID,
    page_url VARCHAR(500),
    user_agent VARCHAR(500),
    ip_address INET,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- User session metrics
CREATE TABLE IF NOT EXISTS analytics.user_sessions (
    session_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    session_duration_minutes INTEGER,
    page_views INTEGER DEFAULT 0,
    add_to_cart_events INTEGER DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    total_purchase_amount DECIMAL(10,2) DEFAULT 0,
    converted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Product metrics
CREATE TABLE IF NOT EXISTS analytics.product_metrics (
    product_id UUID PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(100),
    total_views INTEGER DEFAULT 0,
    total_cart_adds INTEGER DEFAULT 0,
    total_purchases INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5,4) DEFAULT 0,
    revenue DECIMAL(12,2) DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Hourly aggregated metrics
CREATE TABLE IF NOT EXISTS analytics.hourly_metrics (
    hour_timestamp TIMESTAMP WITH TIME ZONE,
    total_events INTEGER DEFAULT 0,
    unique_users INTEGER DEFAULT 0,
    page_views INTEGER DEFAULT 0,
    cart_additions INTEGER DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5,4) DEFAULT 0,
    revenue DECIMAL(12,2) DEFAULT 0,
    PRIMARY KEY (hour_timestamp)
);

-- Real-time dashboard metrics (for latest stats)
CREATE TABLE IF NOT EXISTS analytics.dashboard_metrics (
    metric_key VARCHAR(100) PRIMARY KEY,
    metric_value DECIMAL(15,4),
    metric_label VARCHAR(200),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_raw_events_timestamp ON analytics.raw_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_id ON analytics.raw_events(user_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_type ON analytics.raw_events(event_type);
CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON analytics.user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_start_time ON analytics.user_sessions(start_time);

-- Insert initial dashboard metrics
INSERT INTO analytics.dashboard_metrics (metric_key, metric_value, metric_label) VALUES
('total_users', 0, 'Total Active Users'),
('total_sessions', 0, 'Total Sessions'),
('conversion_rate', 0, 'Overall Conversion Rate (%)'),
('avg_session_duration', 0, 'Average Session Duration (minutes)'),
('total_revenue', 0, 'Total Revenue ($)')
ON CONFLICT (metric_key) DO NOTHING;