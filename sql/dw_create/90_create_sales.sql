-- 90_create_sales.sql

CREATE TABLE IF NOT EXISTS sales (
    transaction_id INTEGER PRIMARY KEY,
    sale_date TEXT,             -- ISO date string, e.g. '2023-05-19'
    customer_id INTEGER,
    product_id INTEGER,
    store_id INTEGER,
    campaign_id INTEGER,
    sale_amount REAL,
    discount_percent REAL,
    payment_type TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
