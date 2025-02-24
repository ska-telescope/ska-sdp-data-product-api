CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    execution_block VARCHAR(255) DEFAULT NULL,
    uid UUID UNIQUE,
    json_hash CHAR(64) UNIQUE
);