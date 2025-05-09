import pandas as pd

from internal.repository import tus
from internal.service.automation.tus_automation import AutomationTusMarks


def tus_excel_upload(file_path: str) -> Exception | str:
    df = pd.read_excel(file_path)
    df.columns = df.columns.str.strip().str.lower()

    df.rename(columns={
        'dvid': 'dvid',
        'reqdt': 'req_date',
        'code': 'code',
        'tus_code': 'tus_code',
        'mark': 'mark'
    }, inplace=True)

    df['dvid'] = pd.to_datetime(df['dvid'], dayfirst=True, errors='coerce')
    df['req_date'] = pd.to_datetime(df['req_date'], dayfirst=True, errors='coerce')

    tus_clean_table()

    resp = tus.tus_excel_upload(df)

    if resp.count("successfully") == 0:
        return Exception("Something went wrong. Please check your xlsx file")

    automation = AutomationTusMarks()

    if automation.set_average_score_owners() is False:
        return Exception("Something went wrong. Please check your xlsx file")

    return resp


def tus_clean_table() -> Exception | str:
    return tus.tus_clean_table()
