-- Customer source table for the CDC path. Debezium captures changes to this
-- table and publishes them to Kafka topic `payments.public.customer`.
CREATE TABLE customer (
    id          INT PRIMARY KEY,
    name        TEXT,
    email       TEXT,
    phone       TEXT,
    country     TEXT,
    status      TEXT
);

-- Ensure full before-images on UPDATE/DELETE (so Debezium emits `before`).
ALTER TABLE customer REPLICA IDENTITY FULL;

INSERT INTO customer (id, name, email, phone, country, status) VALUES
  (101, 'Santosh Nair', 'old@x.com', '111', 'UAE', 'ACTIVE'),
  (102, 'Asha Rao',     'asha@x.com', '222', 'IN',  'ACTIVE');

-- After the stack is up, try:
--   UPDATE customer SET email = 'new@x.com' WHERE id = 101;
--   DELETE FROM customer WHERE id = 102;
-- and watch the events land on the Kafka topic.
