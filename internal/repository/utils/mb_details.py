from internal.app.models.mobile_bank import MobileBank
from internal.lib.date import (
    get_month_date_range
)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def upload_mb_details(cursor, worker_id: int, mb: MobileBank):
    try:
        start_date, end_date = get_month_date_range()

        cursor.execute(
            """
            SELECT id FROM mobile_bank_details
            WHERE worker_id = %s 
              AND inn = %s
              AND created_at >= %s AND created_at < %s
            """,
            (worker_id, mb.inn, start_date, end_date)
        )
        record = cursor.fetchone()

        if record:
            cursor.execute(
                """
                UPDATE mobile_bank_details
                SET inn = %s,
                    prem = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (mb.inn, mb.prem, record[0])
            )
        else:
            cursor.execute(
                """
                INSERT INTO mobile_bank_details(created_at, updated_at, inn, prem, worker_id)
                VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s, %s)
                """,
                (mb.inn, mb.prem, worker_id)
            )

    except Exception as e:
        logger.error("Error in upload_mb_details: {}".format(e))
        raise e

    return True

