from internal.app.models.mobile_bank import MobileBank
from internal.lib.date import (
    get_month_date_range
)
from configs.load_configs import get_config
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def upload_mb_details(cursor, month: int, year: int, worker_id: int, mb: MobileBank):
    try:
        configs = get_config()
        start_date, end_date = get_month_date_range(year, month)
        created_ts = start_date

        cursor.execute(
            """
            SELECT id FROM mobile_bank_details
            WHERE worker_id = %s 
              AND created_at >= %s AND created_at < %s
            """,
            (worker_id, start_date, end_date)
        )
        record = cursor.fetchone()

        if record:
            cursor.execute(
                """
                UPDATE mobile_bank_details
                SET prem = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (configs.service.mobile_bank_prem * mb.connects, created_ts, record[0])
            )
        else:
            cursor.execute(
                """
                INSERT INTO mobile_bank_details(created_at, updated_at, prem, worker_id)
                VALUES (%s, %s, %s, %s)
                """,
                (created_ts, created_ts, configs.service.mobile_bank_prem * mb.connects, worker_id)
            )
    except Exception as e:
        logger.error("Error in upload_mb_details: {}".format(e))
        raise e

    return True

