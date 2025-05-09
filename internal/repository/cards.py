from pandas.core.frame import DataFrame
from psycopg2 import sql

from internal.lib.column_parsers import (parse_date, parse_float)
from internal.lib.encypter import hash_sha256
from pkg.db.connect import (get_connection, get_cursor)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def upload_cards(df: DataFrame, coast_dict: dict) -> Exception | str:
    OP = "repository.upload_cards"

    logger.info("[{}] Uploading cards".format(OP))

    conn = get_connection()
    cursor = get_cursor()

    for _, row in df.iterrows():
        try:
            card_type = str(row['DCL_NAME']).strip()
            coast = coast_dict.get(card_type, 0.0)

            values = (
                parse_date(row['DVID']),  # expire_date
                parse_date(row['REQDT']),  # issue_date
                hash_sha256(card_type),  # card_type
                parse_float(row['IN_BAL_N']),  # in_balance
                parse_float(row['CMOVD_OSD_N']),  # debt_osd
                parse_float(row['CMOVD_OSK_N']),  # debt_osk
                parse_float(row['OUT_BAL']),  # out_balance
                hash_sha256(str(row['TUS_CODE']).strip()),  # owner_name
                coast  # coast
            )

            cursor.execute(
                sql.SQL("""
                INSERT INTO cards (
                    expire_date, issue_date, card_type,
                    in_balance, debt_osd, debt_osk, out_balance,
                    owner_name, coast
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """), values)

        except Exception as e:
            logger.error("[{}] Error while uploading cards: {}".format(OP, str(e)))
            return e

    conn.commit()

    return "Successfully uploaded cards"


def clean_cards_table() -> Exception | str:
    OP = "repository.clean_cards_table"

    logger.info("[{}] Cleaning cards table".format(OP))

    conn = get_connection()
    cursor = get_cursor()

    try:
        cursor.execute(
            sql.SQL("DELETE FROM cards")
        )
        conn.commit()
    except Exception as e:
        logger.error("[{}] Error while cleaning cards table: {}".format(OP, str(e)))
        return e

    logger.info("[{}] Cards table cleaned successfully".format(OP))
    return "Successfully cleaned cards table"
