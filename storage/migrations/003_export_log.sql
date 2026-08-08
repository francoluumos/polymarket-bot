-- Tracks which (table, day) pairs have been exported to Parquet, so the
-- export job is idempotent and empty days don't get retried forever.
CREATE TABLE IF NOT EXISTS export_log (
    table_name  text   NOT NULL,
    day         date   NOT NULL,
    row_count   bigint NOT NULL,
    file        text,
    exported_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, day)
);
