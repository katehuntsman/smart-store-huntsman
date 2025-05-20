"""
scripts/etl_to_dw.py

ETL script to load cleaned data from CSVs into the data warehouse (SQLite).

"""

# === Imports ===
import sys
import pathlib
import sqlite3
import pandas as pd

# Fix path for local imports
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.append(str(PROJECT_ROOT))

# Local import
from utils.logger import logger

# === Path Constants ===
DATA_PREPARED_DIR = PROJECT_ROOT / "data" / "prepared"
DW_PATH = PROJECT_ROOT / "dw" / "smart_sales.sqlite"

# === ETL Function ===
def load_data_to_dw():
    logger.info("ETL STARTED: Loading prepared data into data warehouse.")

    # Check if the DW file exists
    if not DW_PATH.exists():
        logger.error(f"Data warehouse not found at {DW_PATH}. Run create_dw_sqlite.py first.")
        return

    try:
        conn = sqlite3.connect(DW_PATH)

        # Load Customers
        df_customers = pd.read_csv(DATA_PREPARED_DIR / "customers_prepared.csv")
        df_customers = df_customers.rename(columns={
            "CustomerID": "customer_id",
            "Name": "name",
            "Region": "region",
            "JoinDate": "join_date"
        })
        df_customers.to_sql("customers", conn, if_exists="append", index=False)
        logger.info(f"Inserted {len(df_customers)} rows into customers table.")

        # Load Products
        df_products = pd.read_csv(DATA_PREPARED_DIR / "products_prepared.csv")
        df_products = df_products.rename(columns={
            "ProductID": "product_id",
            "ProductName": "product_name",
            "Category": "category",
            "UnitPrice": "unit_price"
        })
        df_products.to_sql("products", conn, if_exists="append", index=False)
        logger.info(f"Inserted {len(df_products)} rows into products table.")

        # Load Sales
        df_sales = pd.read_csv(DATA_PREPARED_DIR / "sales_prepared.csv")
        df_sales = df_sales.rename(columns={
            "TransactionID": "sale_id",
            "SaleDate": "date",
            "CustomerID": "customer_id",
            "ProductID": "product_id",
            "StoreID": "store_id",
            "CampaignID": "campaign_id",
            "SaleAmount": "sales_amount"
        })

        expected_columns = [
            "sale_id", "date", "customer_id",
            "product_id", "quantity", "sales_amount"
        ]
        df_sales = df_sales[[col for col in expected_columns if col in df_sales.columns]]
        df_sales.to_sql("sales", conn, if_exists="append", index=False)
        logger.info(f"Inserted {len(df_sales)} rows into sales table.")

        logger.info("✅ ETL COMPLETED SUCCESSFULLY.")

    except Exception as e:
        logger.error(f"ETL FAILED: {e}")
    finally:
        if conn:
            conn.close()

# === Main Execution ===
if __name__ == "__main__":
    load_data_to_dw()
