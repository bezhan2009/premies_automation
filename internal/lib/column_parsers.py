from datetime import datetime

import pandas as pd


def parse_date(val):
    if pd.isna(val):
        return None
    if isinstance(val, str):
        val = val.strip()
        # Проверяем, соответствует ли строка формату дд.мм.гг (с 2 цифрами года)
        if len(val) == 8 and val.count('.') == 2 and val[2] == '.' and val[5] == '.':
            day, month, year = val.split('.')
            # Преобразуем год из 2 цифр в 4 (добавляем "20")
            val = f"{day}.{month}.20{year}"
        return datetime.strptime(val, '%d.%m.%Y').date()
    return val.date()


def parse_float(val):
    if pd.isna(val):
        return 0.0
    return float(str(val).replace(',', '.'))
