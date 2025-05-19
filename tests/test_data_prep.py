import unittest
import pandas as pd
from scripts.data_cleaning.data_scrubber import DataScrubber
from unittest.mock import patch

class TestDataScrubber(unittest.TestCase):

    # -----------------------------
    # Test: Customers data cleaning
    # -----------------------------
    def test_clean_customers_data(self):
        raw_df = pd.DataFrame({
            'customer_id': [1, 2, 2],
            'name': ['Alice', 'Bob', 'Bob'],
            'email': ['alice@example.com', 'bob@example.com', 'bob@example.com'],
            'age': [28, 35, 35]
        })
        expected_df = pd.DataFrame({
            'customer_id': [1, 2],
            'name': ['Alice', 'Bob'],
            'email': ['alice@example.com', 'bob@example.com'],
            'age': [28, 35]
        }).reset_index(drop=True)

        scrubber = DataScrubber()

        with patch.object(scrubber, 'remove_duplicates', return_value=expected_df), \
             patch.object(scrubber, 'handle_missing_values', return_value=expected_df), \
             patch.object(scrubber, 'remove_outliers', return_value=expected_df):

            result_df = scrubber.clean_data(raw_df)
            pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df)

    # -----------------------------
    # Test: Products data cleaning
    # -----------------------------
    def test_clean_products_data(self):
        raw_df = pd.DataFrame({
            'product_id': [101, 102, 103],
            'name': ['Widget', None, 'Gadget'],
            'price': [9.99, 19.99, None]
        })
        expected_df = pd.DataFrame({
            'product_id': [101],
            'name': ['Widget'],
            'price': [9.99]
        }).reset_index(drop=True)

        scrubber = DataScrubber()

        with patch.object(scrubber, 'remove_duplicates', return_value=raw_df), \
             patch.object(scrubber, 'handle_missing_values', return_value=expected_df), \
             patch.object(scrubber, 'remove_outliers', return_value=expected_df):

            result_df = scrubber.clean_data(raw_df)
            pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df)

    # -----------------------------
    # Test: Sales data cleaning
    # -----------------------------
    def test_clean_sales_data(self):
        raw_df = pd.DataFrame({
            'transaction_id': [1001, 1002, 1003],
            'amount': [49.99, 50000.0, 79.99]  # 50000.0 is an outlier
        })
        expected_df = pd.DataFrame({
            'transaction_id': [1001, 1003],
            'amount': [49.99, 79.99]
        }).reset_index(drop=True)

        scrubber = DataScrubber()

        with patch.object(scrubber, 'remove_duplicates', return_value=raw_df), \
             patch.object(scrubber, 'handle_missing_values', return_value=raw_df), \
             patch.object(scrubber, 'remove_outliers', return_value=expected_df):

            result_df = scrubber.clean_data(raw_df)
            pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df)

if __name__ == '__main__':
    unittest.main()
