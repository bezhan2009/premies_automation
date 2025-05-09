from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def get_users(cursor):
    cursor.execute("SELECT id, username FROM users")
    return cursor.fetchall()


def insert_card_sales(cursor, cards_prem: float, salary_project: float, worker_id: int) -> bool:
    try:
        cursor.execute(
            """
            INSERT INTO card_sales(created_at, updated_at, cards_prem, salary_project, worker_id) 
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s, %s)
            """,
            (cards_prem, salary_project, worker_id)
        )
    except Exception as e:
        logger.error("Error while inserting card sales: {}".format(e))
        return False

    return True


def insert_card_turnovers(cursor, turnovers_prem: float, worker_id: int) -> bool:
    try:
        cursor.execute(
            """
            INSERT INTO card_turnovers(created_at, updated_at, card_turnovers_prem, worker_id)
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s)
            """,
            (turnovers_prem, worker_id)
        )
    except Exception as e:
        logger.error("Error while inserting card turnovers: {}".format(e))
        return False

    return True


def insert_mobile_bank_sales(cursor, mobile_bank_sales_prem: float, worker_id: int) -> bool:
    try:
        cursor.execute(
            """
            INSERT INTO mobile_bank_sales(created_at, updated_at, mobile_bank_prem, worker_id)
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s)
            """,
            (mobile_bank_sales_prem, worker_id)
        )
    except Exception as e:
        logger.error("Error while inserting mobile bank sales: {}".format(e))
        return False

    return True


def insert_tus_marks(cursor, processuses: float, worker_id: int) -> bool:
    try:
        cursor.execute(
            """
            INSERT INTO service_qualities(created_at, updated_at, average_score, worker_id)
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s)
            """,
            (processuses, worker_id)
        )
    except Exception as e:
        logger.error("Error while inserting tus marks: {}".format(e))
        return False

    return True
