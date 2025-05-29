from datetime import datetime, timezone

from internal.lib.date import (
    get_current_year,
    get_current_month
)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def get_workers(cursor):
    cursor.execute("SELECT id, username FROM users WHERE role_id = 2 OR role_id = 6 OR role_id = 8")
    return cursor.fetchall()


def get_month_date_range():
    year, month = get_current_year(), get_current_month()
    start_date = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)

    return start_date, end_date


def upsert_card_sales(cursor, cards_prem: float, salary_project: float, worker_id: int) -> Exception | bool:
    try:
        start_date, end_date = get_month_date_range()

        cursor.execute(
            """
            SELECT id FROM card_sales
            WHERE worker_id = %s
              AND created_at >= %s AND created_at < %s
            """,
            (worker_id, start_date, end_date)
        )
        record = cursor.fetchone()

        if record:
            cursor.execute(
                """
                UPDATE card_sales
                SET cards_prem = %s,
                    salary_project = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (cards_prem, salary_project, record[0])
            )
        else:
            cursor.execute(
                """
                INSERT INTO card_sales(created_at, updated_at, cards_prem, salary_project, worker_id)
                VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s, %s)
                """,
                (cards_prem, salary_project, worker_id)
            )

    except Exception as e:
        logger.error("Error in upsert_card_sales: {}".format(e))
        return e

    return True


def upsert_card_turnovers(cursor, turnovers_prem: float, activations_prem: float, worker_id: int) -> bool:
    try:
        start_date, end_date = get_month_date_range()

        cursor.execute(
            """
            SELECT id FROM card_turnovers
            WHERE worker_id = %s
              AND created_at >= %s AND created_at < %s
            """,
            (worker_id, start_date, end_date)
        )
        record = cursor.fetchone()

        if record:
            cursor.execute(
                """
                UPDATE card_turnovers
                SET card_turnovers_prem = %s,
                    active_cards_perms = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (turnovers_prem, activations_prem, record[0])
            )
        else:
            cursor.execute(
                """
                INSERT INTO card_turnovers(created_at, updated_at, card_turnovers_prem, active_cards_perms, worker_id)
                VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s, %s)
                """,
                (turnovers_prem, activations_prem, worker_id)
            )

    except Exception as e:
        logger.error("Error in upsert_card_turnovers: {}".format(e))
        return False

    return True


def upsert_mobile_bank_sales(cursor, mobile_bank_sales_prem: float, worker_id: int) -> bool:
    try:
        start_date, end_date = get_month_date_range()

        cursor.execute(
            """
            SELECT id FROM mobile_bank_sales
            WHERE worker_id = %s
              AND created_at >= %s AND created_at < %s
            """,
            (worker_id, start_date, end_date)
        )
        record = cursor.fetchone()

        if record:
            cursor.execute(
                """
                UPDATE mobile_bank_sales
                SET mobile_bank_prem = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (mobile_bank_sales_prem, record[0])
            )
        else:
            cursor.execute(
                """
                INSERT INTO mobile_bank_sales(created_at, updated_at, mobile_bank_prem, worker_id)
                VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s)
                """,
                (mobile_bank_sales_prem, worker_id)
            )

    except Exception as e:
        logger.error("Error in upsert_mobile_bank_sales: {}".format(e))
        return False

    return True


def upsert_tus_marks(cursor, processuses: float, worker_id: int) -> bool:
    try:
        start_date, end_date = get_month_date_range()

        cursor.execute(
            """
            SELECT id FROM service_qualities
            WHERE worker_id = %s
              AND created_at >= %s AND created_at < %s
            """,
            (worker_id, start_date, end_date)
        )
        record = cursor.fetchone()

        if record:
            cursor.execute(
                """
                UPDATE service_qualities
                SET call_center = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (processuses, record[0])
            )
        else:
            cursor.execute(
                """
                INSERT INTO service_qualities(created_at, updated_at, call_center, worker_id)
                VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s)
                """,
                (processuses, worker_id)
            )

    except Exception as e:
        logger.error("Error in upsert_tus_marks: {}".format(e))
        return False

    return True
