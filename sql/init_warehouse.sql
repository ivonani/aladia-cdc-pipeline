CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.fct_orders (
    order_id        UUID PRIMARY KEY,
    user_id         UUID,
    status          TEXT,
    amount_cents    BIGINT,
    currency        TEXT,
    country         TEXT,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    last_cdc_ts     TIMESTAMPTZ,
    version         BIGINT,
    extra           JSONB
);

CREATE TABLE IF NOT EXISTS analytics.failed_events (
    id              BIGSERIAL PRIMARY KEY,
    raw_value       TEXT,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);