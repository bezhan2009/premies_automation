import pandas as pd
from pandas.core.frame import DataFrame
from psycopg2 import sql

from internal.lib.encypter import hash_sha256
from pkg.db.connect import (get_connection, get_cursor)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def tus_excel_upload(df: DataFrame) -> Exception | str:
    OP = "repository.tus_excel_upload"

    conn = get_connection()
    cursor = get_cursor()

    for _, row in df.iterrows():
        try:
            cursor.execute(
                sql.SQL("INSERT INTO tus_marks (dvid, req_date, code, tus_code, mark) VALUES (%s, %s, %s, %s, %s)"),
                [
                    row['dvid'].date() if pd.notna(row['dvid']) else None,
                    row['req_date'].date() if pd.notna(row['req_date']) else None,
                    hash_sha256(row['code']),
                    hash_sha256(row['tus_code']),
                    row['mark']
                ]
            )
        except Exception as e:
            logger.error("[{}] Error while uploading tus: {}".format(OP, e))
            return e

    conn.commit()

    logger.info("[{}] Successfully uploaded tus.".format(OP))

    return "Loaded tus data successfully"


def tus_clean_table() -> Exception | str:
    OP = "repository.tus_clean_table"
    conn = get_connection()
    cursor = get_cursor()

    logger.info("[{}] Cleaning tus table".format(OP))

    try:
        cursor.execute(f"DELETE FROM tus_marks")
        conn.commit()
    except Exception as e:
        logger.error("[{}] Error while cleaning table: {}".format(OP, e))
        return e

    logger.info("[{}] Successfully cleaned tus table".format(OP))

    return "Cleaned tus table successfully"
