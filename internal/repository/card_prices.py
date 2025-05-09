from pandas.core.frame import DataFrame
from psycopg2 import sql

from pkg.db.connect import (get_connection, get_cursor)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def upload_card_prices(price_df: DataFrame) -> Exception | str:
    OP = "repository.upload_card_prices"

    logger.info("[{}] Uploading card prices".format(OP))

    conn = get_connection()
    cursor = get_cursor()

    for _, row in price_df.iterrows():
        try:
            cursor.execute(
                sql.SQL("SELECT * FROM card_prices WHERE dcl_name = %s"),
                [row['DCL_NAME']]
            )

            price_row = cursor.fetchone()
            if price_row:
                continue

            cursor.execute(
                sql.SQL("INSERT INTO card_prices(dcl_name, coast) VALUES (%s, %s)"),
                [row['DCL_NAME'], row['Coast']]
            )
        except Exception as e:
            logger.error("[{}] Error while setting the data to card prices table: {}".format(OP, str(e)))
            return e

    conn.commit()

    res_saving_to_dict = upload_card_prices_to_dict()
    if type(res_saving_to_dict) is Exception:
        logger.error(str(res_saving_to_dict))
        return res_saving_to_dict

    return "Successfully uploaded card prices"


def upload_card_prices_to_dict() -> Exception | dict:
    coast_dict = {}

    OP = "repository.upload_card_prices_to_dict"

    logger.info("[{}] Uploading card prices to dict".format(OP))

    cursor = get_cursor()

    try:
        cursor.execute("SELECT * FROM card_prices")
        card_prices = cursor.fetchall()
    except Exception as e:
        logger.error("Error while getting card prices from database: {}".format(str(e)))
        raise  # выбрасываем исключение

    if not card_prices:
        # Вместо возврата Exception, лучше выбросить
        logger.error("No card prices were found")
        raise Exception("No card prices were found")

    for card_price in card_prices:
        coast_dict[str(card_price[1]).strip()] = float(card_price[2])

    return coast_dict
