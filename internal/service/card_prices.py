import pandas as pd

from internal.repository import card_prices

coast_dict = {}


def upload_card_prices(path_file: str):
    clean_card_price_table()

    # Загружаем прайс-лист стоимости карт
    price_df = pd.read_excel(path_file, engine='openpyxl')

    return card_prices.upload_card_prices(price_df)


def upload_card_prices_to_dict():
    global coast_dict

    try:
        res_dict = card_prices.upload_card_prices_to_dict()
    except Exception:
        upload_card_prices("./uploads/prices.xlsx")
        try:
            res_dict = card_prices.upload_card_prices_to_dict()
        except Exception as e2:
            return e2

    coast_dict = res_dict

    return "Successfully uploaded card prices to dictionary."


def clean_card_price_table() -> Exception | str:
    return card_prices.clean_card_prices_table()


def get_coast_dict() -> dict:
    global coast_dict
    return coast_dict
