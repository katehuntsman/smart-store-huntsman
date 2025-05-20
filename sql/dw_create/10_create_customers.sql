-- 10_create_customers.sql

CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    Name TEXT NOT NULL,
    Region TEXT,
    JoinDate TEXT,
    LoyaltyPoints INTEGER,
    CustomerSegment TEXT
);
