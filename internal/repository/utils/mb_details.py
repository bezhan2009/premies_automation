from internal.app.models.mobile_bank import MobileBank
from internal.lib.date import (
    get_month_date_range
)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def upload_mb_details(cursor, month: int, year: int, worker_id: int, mb: MobileBank):
    try:
        start_date, end_date = get_month_date_range(year, month)
        created_ts = start_date

        cursor.execute(
            """
            SELECT id FROM mobile_bank_details
            WHERE worker_id = %s 
              AND connects = %s
              AND created_at >= %s AND created_at < %s
            """,
            (worker_id, mb.connects, start_date, end_date)
        )
        record = cursor.fetchone()

        if record:
            cursor.execute(
                """
                UPDATE mobile_bank_details
                SET connects = %s,
                    prem = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (mb.connects, mb.prem, created_ts, record[0])
            )
        else:
            cursor.execute(
                """
                INSERT INTO mobile_bank_details(created_at, updated_at, connects, prem, worker_id)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (created_ts, created_ts, mb.connects, mb.prem, worker_id)
            )

    except Exception as e:
        logger.error("Error in upload_mb_details: {}".format(e))
        raise e

    return True

