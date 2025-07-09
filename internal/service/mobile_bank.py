import pandas as pd

from internal.repository import mobile_bank
from internal.service.automation.mobile_bank_automation import AutomationMobileBank


def mobile_bank_excel_upload(month: int, year: int, path_file: str) -> Exception | str:
    df = pd.read_excel(path_file)
    df["Количество"] = df["Количество"].fillna(0)

    mobile_bank_clean_table()

    resp = mobile_bank.mobile_bank_excel_upload(df)
    if resp.count("Successfully") == 0:
        raise Exception("Something went wrong. Please check xlsx file")

    automation = AutomationMobileBank()

    if automation.set_mobile_bank_sales(month, year) is False:
        raise Exception("Something went wrong. Please check xlsx file")

    return resp


def mobile_bank_clean_table() -> Exception | str:
    return mobile_bank.mobile_bank_clean_table()
