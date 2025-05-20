"""
scripts/dw_create/create_dw_sqlite.py

This script creates the SQLite data warehouse using .sql files.

File locations:
- data/prepared         : cleaned and prepared CSVs (used later in ETL)
- dw/                   : data warehouse folder
- dw/smart_sales.sqlite : SQLite database file
- sql/dw_create         : folder for .sql files used to create tables

Notes:
- SQLite does not support a native DATE type.
  Dates should be stored as TEXT in ISO format: "YYYY-MM-DD".
"""

import pathlib
import sys
import sqlite3
import pandas as pd

# Add project root to sys.path for local imports
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

# Updated import of logger
from utils.logger import logger

# === Path Constants ===

SCRIPTS_DW_CREATE_DIR = pathlib.Path(__file__).resolve().parent
SCRIPTS_DIR = SCRIPTS_DW_CREATE_DIR.parent
PROJECT_ROOT = SCRIPTS_DIR.parent

DATA_DIR = PROJECT_ROOT / "data"
DATA_PREPARED_DIR = DATA_DIR / "prepared"

SQL_DIR = PROJECT_ROOT / "sql"
SQL_DW_CREATE_DIR = SQL_DIR / "dw_create"

DW_DIR = PROJECT_ROOT / "dw"
DW_PATH = DW_DIR / "smart_sales.sqlite"

DW_DIR.mkdir(parents=True, exist_ok=True)
SQL_DW_CREATE_DIR.mkdir(parents=True, exist_ok=True)

# === Functions ===

def run_sql_file(conn, file_path: pathlib.Path) -> None:
    logger.info(f"RUN: {file_path}")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql = f.read()
            conn.executescript(sql)
    except Exception as e:
        logger.error(f"Error running {file_path.name}: {e}")
        raise

def create_dw() -> None:
    logger.info("Creating data warehouse using SQLite...")

    sql_files = list(SQL_DW_CREATE_DIR.glob("*.sql"))
    if not sql_files:
        logger.error(f"No .sql files in {SQL_DW_CREATE_DIR}. Please add schema files before running.")
        exit(22)

    conn = None
    try:
        conn = sqlite3.connect(DW_PATH)

        run_sql_file(conn, SQL_DW_CREATE_DIR / "00_drop_all_tables.sql")
        run_sql_file(conn, SQL_DW_CREATE_DIR / "10_create_customers.sql")
        run_sql_file(conn, SQL_DW_CREATE_DIR / "20_create_products.sql")
        run_sql_file(conn, SQL_DW_CREATE_DIR / "90_create_sales.sql")

        logger.info("Data warehouse schema created successfully.")

    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        exit(23)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        exit(24)
    finally:
        if conn:
            conn.close()

def populate_dw() -> None:
    logger.info("Populating SQLite data warehouse from prepared CSVs...")

    conn = None
    try:
        conn = sqlite3.connect(DW_PATH)

        # --- Customers ---
        df_customers = pd.read_csv(DATA_PREPARED_DIR / "customers_prepared.csv")
        # Rename columns from CSV names (CamelCase) to DB column names (lowercase with underscores)
        df_customers = df_customers.rename(columns={
            "CustomerID": "customer_id",
            "Name": "name",
            "Region": "region",
             "JoinDate": "JoinDate",
            "LoyaltyPoints": "LoyaltyPoints",
            "CustomerSegment": "CustomerSegment"
        })

        # Drop duplicates based on primary key 'customer_id'
        df_customers = df_customers.drop_duplicates(subset=['customer_id'])
        
        df_customers.to_sql("customers", conn, if_exists="append", index=False)
        logger.info(f"Inserted {len(df_customers)} rows into customers table.")

        # --- Products ---
        df_products = pd.read_csv(DATA_PREPARED_DIR / "products_prepared.csv")

        # Drop rows with missing productname (required by schema)
        df_products = df_products.dropna(subset=["productname"])

        # Rename to match DB column names
        df_products = df_products.rename(columns={
            "productid": "product_id",
            "productname": "product_name",
            "category": "category",
            "unitprice": "price",
            "stockquantity": "stock_quantity"
        })

        # Keep only columns defined in the schema
        df_products = df_products[["product_id", "product_name", "category", "price", "stock_quantity"]]

        # Insert into DB
        df_products.to_sql("products", conn, if_exists="append", index=False)
        logger.info(f"Inserted {len(df_products)} rows into products table.")

        # --- Sales ---
        df_sales = pd.read_csv(DATA_PREPARED_DIR / "sales_prepared.csv")
        df_sales = df_sales.rename(columns={
            "TransactionID": "transaction_id",
            "SaleDate": "sale_date",
            "CustomerID": "customer_id",
            "ProductID": "product_id",
            "StoreID": "store_id",
            "CampaignID": "campaign_id",
            "SaleAmount": "sale_amount",
            "DiscountPercent": "discount_percent",
            "PaymentType": "payment_type"
        })

        # Drop duplicate transaction IDs to avoid UNIQUE constraint errors
        df_sales = df_sales.drop_duplicates(subset=["transaction_id"])

        expected_columns = [
            "transaction_id",
            "sale_date",
            "customer_id",
            "product_id",
            "store_id",
            "campaign_id",
            "sale_amount",
            "discount_percent",
            "payment_type"
        ]

        # Keep only expected columns
        df_sales = df_sales[[col for col in expected_columns if col in df_sales.columns]]

        df_sales.to_sql("sales", conn, if_exists="append", index=False)
        logger.info(f"Inserted {len(df_sales)} rows into sales table.")

        logger.info("All tables populated successfully.")

    except Exception as e:
        logger.error(f"Error populating SQLite data warehouse: {e}")
        exit(1)
    finally:
        if conn:
            conn.close()

def main() -> None:
    logger.info("========================================")
    logger.info("Starting: create_dw_sqlite.py")
    logger.info("========================================")
    logger.info(f"Root:   {PROJECT_ROOT}")
    logger.info(f"Scripts:{SCRIPTS_DW_CREATE_DIR}")
    logger.info(f"SQL:    {SQL_DW_CREATE_DIR}")
    logger.info(f"Input:  {DATA_PREPARED_DIR}")
    logger.info(f"Output: {DW_PATH}")
    create_dw()
    populate_dw()
    logger.info("========================================")
    logger.info("Finished: create_dw_sqlite.py")
    logger.info("========================================")

if __name__ == "__main__":
    main()
