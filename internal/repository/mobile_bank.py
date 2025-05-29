from pandas.core.frame import DataFrame
from psycopg2 import sql

from internal.lib.encypter import hash_sha256
from pkg.db.connect import (get_connection, get_cursor)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def mobile_bank_excel_upload(df: DataFrame) -> Exception | str:
    OP = "repository.mobile_bank_excel_upload"
    conn = get_connection()
    cursor = get_cursor()

    for _, row in df.iterrows():
        try:
            cursor.execute(
                sql.SQL("INSERT INTO mobile_bank (surname, inn) VALUES (%s, %s)"),
                [hash_sha256(row['surname']), hash_sha256(row['inn'])]
            )
        except Exception as e:
            logger.error("[{}] Error while setting the data to mobile bank table: {}".format(OP, str(e)))
            return e

    conn.commit()

    return "Successfully Uploaded mobile bank data"


def mobile_bank_clean_table() -> Exception | str:
    OP = "repository.mobile_bank_clean_table"
    conn = get_connection()
    cursor = get_cursor()

    logger.info("[{}] Cleaning mobile bank table".format(OP))

    try:
        cursor.execute(f"DELETE FROM mobile_bank")
        conn.commit()
    except Exception as e:
        logger.error("[{}] Error while cleaning mobile bank table: {}".format(OP, str(e)))
        raise e

    logger.info("[{}] Successfully cleaned mobile bank table".format(OP))

    return "Cleaned mobile bank table successfully"
