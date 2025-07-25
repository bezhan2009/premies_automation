from internal.app.models.card import Card
from internal.lib.date import (
    get_month_date_range
)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def get_workers(cursor):
    cursor.execute("SELECT id, full_name FROM users WHERE role_id = 2 OR role_id = 6 OR role_id = 8")
    return cursor.fetchall()


def upsert_card_sales(cursor, month: int, year: int, cards_sailed: int, cards_prem: float, card, worker_id: int) -> Exception | bool:
    """
    Upsert card sales for a given worker, month and year.
    """
    try:
        start_date, end_date = get_month_date_range(year, month)
        created_ts = start_date

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
                SET cards_sailed = %s,
                    cards_prem = %s,
                    cards_sailed_in_general = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    cards_sailed,
                    cards_prem,
                    card.cards_sailed_in_general,
                    created_ts,
                    record[0]
                )
            )
        else:
            cursor.execute(
                """
                INSERT INTO card_sales(
                    created_at,
                    updated_at,
                    cards_sailed,
                    cards_prem,
                    cards_sailed_in_general,
                    worker_id
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    created_ts,
                    created_ts,
                    cards_sailed,
                    cards_prem,
                    card.cards_sailed_in_general,
                    worker_id
                )
            )

    except Exception as e:
        logger.error(f"Error in upsert_card_sales: {e}")
        return e

    return True


def upsert_card_details(cursor, month: int, year: int, card_details, worker_id: int, worker_name: str) -> bool:
    """
    Upsert card details for a given worker, month and year.
    """
    try:
        start_date, end_date = get_month_date_range(year, month)
        created_ts = start_date

        cursor.execute(
            """
                SELECT id FROM card_details
                WHERE code = %s
            """,
            (
                card_details.code,
            )
        )

        record = cursor.fetchone()

        if record:
            cursor.execute(
                """
                UPDATE card_details
                SET expire_date = %s,
                    in_balance = %s,
                    debt_osd = %s,
                    debt_osk = %s,
                    out_balance = %s,
                    coast_cards = %s,
                    coast_credits = %s,
                    card_type = %s,
                    code = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    card_details.expire_date,
                    card_details.in_balance,
                    card_details.debt_osd,
                    card_details.debt_osk,
                    card_details.out_balance,
                    card_details.coast_cards,
                    card_details.coast_credits,
                    card_details.card_type,
                    card_details.code,
                    created_ts,
                    record[0]
                )
            )
        else:
            cursor.execute(
                """
                INSERT INTO card_details(
                    expire_date,
                    issue_date,
                    card_type,
                    code,
                    in_balance,
                    debt_osd,
                    debt_osk,
                    out_balance,
                    coast_cards,
                    coast_credits,
                    created_at,
                    updated_at,
                    worker_id,
                    owner_name
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    card_details.expire_date,
                    card_details.issue_date,
                    card_details.card_type,
                    card_details.code,
                    card_details.in_balance,
                    card_details.debt_osd,
                    card_details.debt_osk,
                    card_details.out_balance,
                    card_details.coast_cards,
                    card_details.coast_credits,
                    created_ts,
                    created_ts,
                    worker_id,
                    worker_name
                )
            )
    except Exception as e:
        logger.error(f"Error in upsert_card_details: {e}")
        raise e

    return True


def upsert_card_turnovers(cursor, month: int, year: int, activated_cards: int, turnovers_prem: float, activations_prem: float, worker_id: int) -> bool:
    """
    Upsert card turnovers for a given worker, month and year.
    """
    try:
        start_date, end_date = get_month_date_range(year, month)
        created_ts = start_date

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
                    activated_cards = %s,
                    active_cards_perms = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (turnovers_prem, activated_cards, activations_prem, created_ts, record[0])
            )
        else:
            cursor.execute(
                """
                INSERT INTO card_turnovers(
                    created_at,
                    updated_at,
                    card_turnovers_prem,
                    activated_cards,
                    active_cards_perms,
                    worker_id
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (created_ts, created_ts, turnovers_prem, activated_cards, activations_prem, worker_id)
            )

    except Exception as e:
        logger.error(f"Error in upsert_card_turnovers: {e}")
        return False

    return True


def upsert_mobile_bank_sales(cursor, month: int, year: int, mobile_bank_sales_prem: float, mobile_bank_connects: int, worker_id: int) -> bool:
    try:
        start_date, end_date = get_month_date_range(year, month)
        created_ts = start_date

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
                    mobile_bank_connects = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (mobile_bank_sales_prem, mobile_bank_connects, created_ts, record[0])
            )
        else:
            cursor.execute(
                """
                INSERT INTO mobile_bank_sales(created_at, updated_at, mobile_bank_prem, mobile_bank_connects, worker_id)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (created_ts, created_ts, mobile_bank_sales_prem, mobile_bank_connects, worker_id)
            )

    except Exception as e:
        logger.error("Error in upsert_mobile_bank_sales: {}".format(e))
        return False

    return True


def upsert_tus_marks(cursor, month: int, year: int, processes: float, tests: float, complaints: int, worker_id: int) -> bool:
    try:
        start_date, end_date = get_month_date_range(year, month)
        created_ts = start_date

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
            update_fields = []
            update_values = []

            if processes != 0:
                update_fields.append("call_center = %s")
                update_values.append(processes)

            if tests != 0:
                update_fields.append("tests = %s")
                update_values.append(tests)

            if complaints != 0:
                update_fields.append("complaint = %s")
                update_values.append(complaints)

            # обновляем только если есть что обновлять
            if update_fields:
                update_fields.append("updated_at = %s")
                update_values.append(created_ts)
                update_values.append(record[0])

                query = f"""
                    UPDATE service_qualities
                    SET {', '.join(update_fields)}
                    WHERE id = %s
                """
                cursor.execute(query, tuple(update_values))

        else:
            # если записи нет, вставляем все значения (вставляем даже если какие-то = 0)
            cursor.execute(
                """
                INSERT INTO service_qualities(created_at, updated_at, call_center, tests, complaint, worker_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (created_ts, created_ts, processes, tests, complaints, worker_id)
            )

    except Exception as e:
        logger.error("Error in upsert_tus_marks: {}".format(e))
        return False

    return True
