from datetime import datetime

import pandas as pd


def parse_date(val):
    if pd.isna(val):
        return None
    if isinstance(val, str):
        return datetime.strptime(val.strip(), '%d.%m.%Y').date()
    return val.date()


def parse_float(val):
    if pd.isna(val):
        return 0.0
    return float(str(val).replace(',', '.'))
