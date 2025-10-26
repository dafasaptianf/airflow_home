CREATE TABLE IF NOT EXISTS retail_transactions (
  id TEXT PRIMARY KEY,
  customer_id TEXT,
  last_status TEXT,
  pos_origin TEXT,
  pos_destination TEXT,
  created_at timestamptz,
  updated_at timestamptz,
  deleted_at timestamptz
);

INSERT INTO retail_transactions (id, customer_id, last_status, pos_origin, pos_destination, created_at, updated_at, deleted_at)
VALUES
  ('r001','c001','IN_TRANSIT','Jakarta','Bandung', now() - interval '2 hour', now() - interval '2 hour', NULL),
  ('r002','c002','DELIVERED','Jakarta','Surabaya', now() - interval '10 day', now() - interval '1 hour', NULL),
  ('r003','c003','DONE','Jakarta','Yogyakarta', now() - interval '5 day', now() - interval '3 hour', now() - interval '1 hour');
