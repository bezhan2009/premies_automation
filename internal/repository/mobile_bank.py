from contextlib import closing
from pandas.core.frame import DataFrame
from psycopg2 import sql

from internal.lib.encypter import hash_sha256
from pkg.db.connect import (get_connection, get_cursor)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def mobile_bank_excel_upload(df: DataFrame) -> Exception | str:
    OP = "repository.mobile_bank_excel_upload"
    conn = get_connection()

    try:
        with closing(get_cursor()) as cursor:
            for _, row in df.iterrows():
                try:
                    cursor.execute(
                        sql.SQL("""
                            INSERT INTO mobile_bank 
                            (surname, mobile_bank_connects) 
                            VALUES (%s, %s)
                        """),
                        [hash_sha256(row['ФИО']), int(row['Количество'])]
                    )
                except Exception as e:
                    logger.error(
                        "[{}] Error processing row for {}: {}".format(
                            OP,
                            row['ФИО'],
                            str(e)
                        )
                    )
                    continue

        conn.commit()
        return "Successfully uploaded mobile bank data"

    except Exception as e:
        conn.rollback()
        logger.error("[{}] Transaction failed: {}".format(OP, str(e)))
        return e


def mobile_bank_clean_table() -> Exception | str:
    OP = "repository.mobile_bank_clean_table"
    conn = get_connection()

    logger.info("[{}] Cleaning mobile bank table".format(OP))

    try:
        with closing(get_cursor()) as cursor:
            cursor.execute(
                sql.SQL("DELETE FROM mobile_bank")
            )

        conn.commit()
        logger.info("[{}] Successfully cleaned mobile bank table".format(OP))
        return "Cleaned mobile bank table successfully"

    except Exception as e:
        conn.rollback()
        logger.error("[{}] Error while cleaning mobile bank table: {}".format(OP, str(e)))
        raise e
