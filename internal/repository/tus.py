import pandas as pd
from contextlib import closing
from pandas.core.frame import DataFrame
from psycopg2 import sql

from internal.lib.encypter import hash_sha256
from pkg.db.connect import (get_connection, get_cursor)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def tus_excel_upload(df: DataFrame) -> Exception | str:
    OP = "repository.tus_excel_upload"
    conn = get_connection()

    try:
        with closing(get_cursor()) as cursor:
            for _, row in df.iterrows():
                try:
                    cursor.execute(
                        sql.SQL("""
                            INSERT INTO tus_marks 
                            (surname, mark, tests, complaints) 
                            VALUES (%s, %s, %s, %s)
                        """),
                        [
                            hash_sha256(str(row['ФИО'])),
                            row['БАЛЛ'],
                            row['ОЦЕНКА'],
                            row['ЖАЛОБЫ'],
                        ]
                    )
                except Exception as e:
                    logger.error(
                        "[{}] Error processing row for {}: {}".format(
                            OP,
                            row['ФИО'],
                            str(e)
                        )
                    )
                    # Продолжаем обработку следующих строк при ошибке
                    continue

        conn.commit()
        logger.info("[{}] Successfully uploaded TUS data".format(OP))
        return "Loaded TUS data successfully"

    except Exception as e:
        conn.rollback()
        logger.error("[{}] Transaction failed: {}".format(OP, str(e)))
        return e


def tus_clean_table() -> Exception | str:
    OP = "repository.tus_clean_table"
    conn = get_connection()

    logger.info("[{}] Cleaning TUS table".format(OP))

    try:
        with closing(get_cursor()) as cursor:
            cursor.execute(
                sql.SQL("DELETE FROM tus_marks")
            )

        conn.commit()
        logger.info("[{}] Successfully cleaned TUS table".format(OP))
        return "Cleaned TUS table successfully"

    except Exception as e:
        conn.rollback()
        logger.error("[{}] Error while cleaning table: {}".format(OP, str(e)))
        raise e
