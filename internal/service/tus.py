import pandas as pd

from internal.repository import tus
from internal.service.automation.tus_automation import AutomationTusMarks


def tus_excel_upload(month: int, year: int, file_path: str) -> Exception | str:
    df = pd.read_excel(file_path)

    # Убедиться, что нужные колонки есть, если нет — создать со значением 0
    required_columns_with_defaults = {
        'БАЛЛ': 0.0,
        'ОЦЕНКА': 0.0,
        'ЖАЛОБЫ': 0
    }

    for column, default_value in required_columns_with_defaults.items():
        if column not in df.columns:
            df[column] = default_value

    # Заменить NaN на 0 в целевых колонках
    df[list(required_columns_with_defaults.keys())] = df[list(required_columns_with_defaults.keys())].fillna(0)

    # Очистка таблицы перед загрузкой
    tus_clean_table()

    # Загрузка данных
    resp = tus.tus_excel_upload(df)

    if resp.count("successfully") == 0:
        return Exception("Something went wrong. Please check your xlsx file")

    # Запуск автоматической логики
    automation = AutomationTusMarks()
    if automation.set_average_score_owners(month, year) is False:
        return Exception("Something went wrong. Please check your xlsx file")

    return resp


def tus_clean_table() -> Exception | str:
    return tus.tus_clean_table()
