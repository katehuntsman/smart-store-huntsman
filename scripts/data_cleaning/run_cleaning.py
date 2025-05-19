import pandas as pd

class DataScrubber:
    def __init__(self):
        pass

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates()

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna()

    def remove_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_cols = df.select_dtypes(include=['number']).columns
        if numeric_cols.empty:
            return df

        clean_df = df.copy()
        for col in numeric_cols:
            Q1 = clean_df[col].quantile(0.25)
            Q3 = clean_df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            clean_df = clean_df[(clean_df[col] >= lower_bound) & (clean_df[col] <= upper_bound)]

        return clean_df
