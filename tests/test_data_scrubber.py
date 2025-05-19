# tests/test_data_scrubber.py

import unittest
import pandas as pd
from utils.data_scrubber import DataScrubber

class TestDataScrubber(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            'customer_id': [1, 2, 2, 3, None],
            'customer_name': ['Alice ', 'BOB', 'BOB', 'Charlie', 'David'],
            'age': [25, 30, 30, None, 22]
        })
        self.scrubber = DataScrubber(self.df)

    def test_remove_duplicate_records(self):
        self.scrubber.remove_duplicate_records()
        self.assertEqual(len(self.scrubber.df), 4)  # One duplicate removed

    def test_handle_missing_data_drop(self):
        self.scrubber.handle_missing_data(drop=True)
        self.assertFalse(self.scrubber.df.isnull().any().any())

    def test_format_column_strings_to_lower_and_trim(self):
        self.scrubber.format_column_strings_to_lower_and_trim('customer_name')
        expected = ['alice', 'bob', 'bob', 'charlie', 'david']
        self.assertListEqual(list(self.scrubber.df['customer_name']), expected)

    def test_convert_column_to_new_data_type(self):
        self.scrubber.handle_missing_data(fill_value=0)
        self.scrubber.convert_column_to_new_data_type('customer_id', int)
        self.assertTrue(pd.api.types.is_integer_dtype(self.scrubber.df['customer_id']))

if __name__ == '__main__':
    unittest.main()
