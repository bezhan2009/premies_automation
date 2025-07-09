import pandas as pd

from internal.repository import tus
from internal.service.automation.tus_automation import AutomationTusMarks


def tus_excel_upload(month: int, year: int, file_path: str) -> Exception | str:
    df = pd.read_excel(file_path)

    df.rename(columns={
        'ФИО': 'ФИО',
        'БАЛЛ': 'БАЛЛ'
    }, inplace=True)

    tus_clean_table()

    resp = tus.tus_excel_upload(df)

    if resp.count("successfully") == 0:
        return Exception("Something went wrong. Please check your xlsx file")

    automation = AutomationTusMarks()

    if automation.set_average_score_owners(month, year) is False:
        return Exception("Something went wrong. Please check your xlsx file")

    return resp


def tus_clean_table() -> Exception | str:
    return tus.tus_clean_table()
