from typing import Tuple
import pandas as pd


def validate_df(df) -> Tuple[pd.DataFrame, bool]:
    is_valid = ~(df["status_code"] != 200).any()
    return df, is_valid