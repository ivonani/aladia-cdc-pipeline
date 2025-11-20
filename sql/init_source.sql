CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Operational orders table
CREATE TABLE IF NOT EXISTS orders (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id      UUID NOT NULL,
    status       TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    currency     TEXT NOT NULL,
    country      TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    version      BIGINT NOT NULL DEFAULT 0
);

-- CDC log table
CREATE TABLE IF NOT EXISTS orders_cdc_log (
    cdc_id     BIGSERIAL PRIMARY KEY,
    event_id   UUID NOT NULL DEFAULT uuid_generate_v4(),
    order_id   UUID NOT NULL,
    op         CHAR(1) NOT NULL,  -- c = create, u = update, d = delete
    version    BIGINT NOT NULL,
    payload    JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_cdc_log_created_at
    ON orders_cdc_log(created_at);

CREATE INDEX IF NOT EXISTS idx_orders_cdc_log_cdc_id
    ON orders_cdc_log(cdc_id);

-- Trigger: write every change to CDC log
CREATE OR REPLACE FUNCTION log_orders_cdc() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO orders_cdc_log(order_id, op, version, payload)
        VALUES (NEW.id, 'c', NEW.version, to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        NEW.version := OLD.version + 1;
        NEW.updated_at := now();
        INSERT INTO orders_cdc_log(order_id, op, version, payload)
        VALUES (NEW.id, 'u', NEW.version, to_jsonb(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO orders_cdc_log(order_id, op, version, payload)
        VALUES (OLD.id, 'd', OLD.version, to_jsonb(OLD));
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS orders_cdc_trg ON orders;
CREATE TRIGGER orders_cdc_trg
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION log_orders_cdc();