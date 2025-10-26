CREATE TABLE IF NOT EXISTS retail_transactions_dw (
  id TEXT PRIMARY KEY,
  customer_id TEXT,
  last_status TEXT,
  pos_origin TEXT,
  pos_destination TEXT,
  created_at timestamptz,
  updated_at timestamptz,
  deleted_at timestamptz
);

CREATE TABLE IF NOT EXISTS json_runtime_log (
  id TEXT,
  runtime_date timestamptz,
  load_time NUMERIC,
  message TEXT
);