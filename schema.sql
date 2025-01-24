DROP TABLE IF EXISTS pending_messages;
DROP TABLE IF EXISTS completed_messages;
DROP TABLE IF EXISTS failed_messages;
DROP TABLE IF EXISTS queues;

CREATE TABLE IF NOT EXISTS queues (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  url VARCHAR(2048) NOT NULL,
  method VARCHAR(10) NOT NULL,
  headers JSON DEFAULT NULL,
  max_retries INTEGER NOT NULL DEFAULT 5,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(name, url, method, max_retries)
);

CREATE TABLE pending_messages (
  id SERIAL PRIMARY KEY,
  queue_id INTEGER NOT NULL REFERENCES queues(id),
  headers JSON,
  payload BYTEA DEFAULT NULL,
  retries INTEGER DEFAULT 0,
  schedule VARCHAR(100) DEFAULT NULL,
  execute_at TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE completed_messages (
  id INTEGER,
  queue_id INTEGER NOT NULL REFERENCES queues(id),
  headers JSON,
  payload BYTEA DEFAULT NULL,
  retries INTEGER DEFAULT 0,
  response TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE failed_messages (
  id INTEGER,
  queue_id INTEGER NOT NULL REFERENCES queues(id),
  headers JSON,
  payload BYTEA DEFAULT NULL,
  retries INTEGER DEFAULT 0,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);