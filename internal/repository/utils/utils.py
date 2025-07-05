from internal.app.models.card import Card
from internal.lib.date import (
    get_month_date_range
)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def get_workers(cursor):
    cursor.execute("SELECT id, username FROM users WHERE role_id = 2 OR role_id = 6 OR role_id = 8")
    return cursor.fetchall()


def upsert_card_sales(cursor, cards_sailed: int, cards_prem: float, card: Card, worker_id: int) -> Exception | bool:
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
                SET cards_sailed = %s,
                    cards_prem = %s,
                    deb_osd = %s,
                    deb_osk = %s,
                    in_balance = %s,
                    out_balance = %s,
                    cards_sailed_in_general = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (
                    cards_sailed,
                    cards_prem,
                    card.debt_osd,
                    card.debt_osk,
                    card.in_balance,
                    card.out_balance,
                    card.cards_sailed_in_general,
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
                    deb_osd, 
                    deb_osk, 
                    in_balance, 
                    out_balance,
                    cards_sailed_in_general,
                    worker_id
                )
                VALUES (
                    CURRENT_TIMESTAMP, 
                    CURRENT_TIMESTAMP, 
                    %s, 
                    %s, 
                    %s, 
                    %s, 
                    %s, 
                    %s, 
                    %s, 
                    %s
                )
                """,
                (
                    cards_sailed,
                    cards_prem,
                    card.debt_osd,
                    card.debt_osk,
                    card.in_balance,
                    card.out_balance,
                    card.cards_sailed_in_general,
                    worker_id
                )
            )

    except Exception as e:
        logger.error("Error in upsert_card_sales: {}".format(e))
        return e

    return True


def upsert_card_details(cursor, card_details: Card, worker_id: int):
    try:
        start_date, end_date = get_month_date_range()

        # Используем только ключевые поля, по которым можно определить уникальность
        cursor.execute(
            """
                SELECT id FROM card_details
                WHERE worker_id = %s
                  AND created_at >= %s AND created_at < %s
                  AND issue_date = %s
                  AND card_type = %s
                  AND code = %s
            """,
            (
                worker_id,
                start_date,
                end_date,
                card_details.issue_date,
                card_details.card_type,
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
                    coast = %s,
                    card_type = %s,
                    code = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (
                    card_details.expire_date,
                    card_details.in_balance,
                    card_details.debt_osd,
                    card_details.debt_osk,
                    card_details.out_balance,
                    card_details.coast,
                    card_details.card_type,
                    card_details.code,
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
                    coast,
                    created_at,
                    updated_at,
                    worker_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s)
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
                    card_details.coast,
                    worker_id
                )
            )
    except Exception as e:
        logger.error("Error in upsert_card_details: {}".format(e))
        raise e

    return True


def upsert_card_turnovers(cursor, activated_cards: int, turnovers_prem: float, activations_prem: float, worker_id: int) -> bool:
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
                    activated_cards = %s,
                    active_cards_perms = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (turnovers_prem, activated_cards, activations_prem, record[0])
            )
        else:
            cursor.execute(
                """
                INSERT INTO card_turnovers(created_at, updated_at, card_turnovers_prem, activated_cards, active_cards_perms, worker_id)
                VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s, %s, %s)
                """,
                (turnovers_prem, activated_cards, activations_prem, worker_id)
            )

    except Exception as e:
        logger.error("Error in upsert_card_turnovers: {}".format(e))
        return False

    return True


def upsert_mobile_bank_sales(cursor, mobile_bank_sales_prem: float, mobile_bank_connects: int, worker_id: int) -> bool:
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
                    mobile_bank_connects = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (mobile_bank_sales_prem, mobile_bank_connects, record[0])
            )
        else:
            cursor.execute(
                """
                INSERT INTO mobile_bank_sales(created_at, updated_at, mobile_bank_prem, mobile_bank_connects, worker_id)
                VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s, %s)
                """,
                (mobile_bank_sales_prem, mobile_bank_connects, worker_id)
            )

    except Exception as e:
        logger.error("Error in upsert_mobile_bank_sales: {}".format(e))
        return False

    return True


def upsert_tus_marks(cursor, processes: float, worker_id: int) -> bool:
    try:
        start_date, end_date = get_month_date_range()

        print(processes)

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
                (processes, record[0])
            )
        else:
            cursor.execute(
                """
                INSERT INTO service_qualities(created_at, updated_at, call_center, worker_id)
                VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s)
                """,
                (processes, worker_id)
            )
    except Exception as e:
        logger.error("Error in upsert_tus_marks: {}".format(e))
        return False

    return True
