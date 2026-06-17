-- Enable pgcrypto for gen_random_uuid
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Events table: one row per step execution (or parallel summary)
CREATE TABLE IF NOT EXISTS pipeline_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    correlation_id TEXT,
    pipeline_name TEXT,
    step_name TEXT,
    event_type TEXT,
    payload JSONB,
    status TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    duration_ms NUMERIC,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_events_correlation ON pipeline_events(correlation_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_events_pipeline ON pipeline_events(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_events_created_at ON pipeline_events(created_at);

-- Pipelines summary table
CREATE TABLE IF NOT EXISTS pipelines (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    correlation_id TEXT,
    pipeline_name TEXT,
    payload JSONB,
    status TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    duration_ms NUMERIC,
    errors JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pipelines_correlation ON pipelines(correlation_id);
CREATE INDEX IF NOT EXISTS idx_pipelines_pipeline ON pipelines(pipeline_name);
