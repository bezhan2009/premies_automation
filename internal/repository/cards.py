from datetime import datetime

from pandas.core.frame import DataFrame
from psycopg2 import sql

from internal.lib.column_parsers import (parse_date, parse_float)
from internal.lib.date import get_month_date_range
from internal.lib.encypter import hash_sha256
from internal.sql.general import (get_worker_id_by_owner_name,
                                  get_card_by_code)
from internal.app.models.card import Card
from pkg.db.connect import (get_connection, get_cursor)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def upload_cards(df: DataFrame, coast_dict: dict, month: int, year: int) -> Exception | str:
    OP = "repository.upload_cards"

    logger.info("[{}] Uploading cards".format(OP))

    conn = get_connection()
    cursor = get_cursor()

    start_date, end_date = get_month_date_range(year, month)
    created_ts = start_date

    for _, row in df.iterrows():
        try:
            upload_cards_stats(
                month,
                year,
                parse_float(row['Оборот ДТ']),  # debt_osd
                parse_float(row['Оборот КТ']),  # debt_osk
                parse_float(row['Исх остаток']),  # out_balance
                parse_float(row['Вх остаток '])  # in_balance
            )

            cursor.execute(
                sql.SQL(get_worker_id_by_owner_name),
                {
                    "owner_name": str(row['Менеджер выпуска карты']).strip()
                }
            )

            worker_id = cursor.fetchone()
            if worker_id is None:
                # logger.warning("[{}] Failed to find worker id: {}".format(OP, str(row['Менеджер выпуска карты']).strip()))
                continue

            card = Card(
                debt_osd=parse_float(row['Оборот ДТ']),
                debt_osk=parse_float(row['Оборот КТ']),
                out_balance=parse_float(row['Исх остаток']),
                in_balance=parse_float(row['Вх остаток ']),

                coast_cards=float(),
                coast_credits=float(),
                card_type="",
                cards_sailed_in_general=int(),
                expire_date=datetime.now(),
                issue_date=datetime.now(),
                owner_name="",
                code=""
            )

            upload_card_sales(
                cursor, month, year, card, worker_id[0]
            )

            cursor.execute(
                sql.SQL(get_card_by_code),
                {
                    "card_code": str(row['Счёт ПК'])
                }
            )

            card = cursor.fetchone()
            if card is not None:
                # logger.warning("[{}] Card already exists: {}".format(OP, str(row['Счёт ПК'])))
                continue

            card_type = str(row['Продукт']).strip()
            coast = coast_dict.get(card_type, [0.0, 0.0])

            values = (
                parse_date(row['Дата выпуска']),  # expire_date
                parse_date(row['Дата выпуска']),  # issue_date
                card_type,  # card_type
                str(row['Счёт ПК']),  # code
                parse_float(row['Вх остаток ']),  # in_balance
                parse_float(row['Оборот ДТ']),  # debt_osd
                parse_float(row['Оборот КТ']),  # debt_osk
                parse_float(row['Исх остаток']),  # out_balance
                hash_sha256(str(row['Менеджер выпуска карты']).strip()),  # owner_name
                coast[0],
                coast[1],
                worker_id[0],
                created_ts,
                created_ts
            )

            cursor.execute(
                sql.SQL("""
                INSERT INTO card_details (
                    expire_date, issue_date, card_type, code,
                    in_balance, debt_osd, debt_osk, out_balance,
                    owner_name, coast_cards, coast_credits, worker_id, 
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """), values)
        except Exception as e:
            logger.error("[{}] Error while uploading cards: {}".format(OP, str(e)))
            return e

    conn.commit()

    return "Successfully uploaded cards"


def upload_cards_stats(month: int, year: int, debt_osd: float, debt_osk: float, out_balance: float, in_balance: float) -> bool:
    OP = "repository.upload_cards_stats"

    conn = get_connection()
    cursor = get_cursor()
    try:
        start_date, end_date = get_month_date_range(year, month)
        created_ts = start_date

        cursor.execute(
            sql.SQL(
                """
                SELECT * FROM cards_stats
                WHERE created_at >= %s AND created_at < %s
                """
            ),
            (start_date, end_date)
        )

        cards_stats = cursor.fetchone()
        if cards_stats is not None:
            cursor.execute(
                sql.SQL(
                    """
                    UPDATE cards_stats
                    SET debt_osd = debt_osd + %s, 
                        debt_osk = debt_osk + %s,
                        out_balance = out_balance + %s,
                        in_balance = in_balance + %s
                    WHERE created_at >= %s AND created_at < %s
                    """
                ),
                (debt_osd, debt_osk, out_balance, in_balance, start_date, end_date)
            )
        else:
            cursor.execute(
                sql.SQL(
                    """
                    INSERT INTO cards_stats (created_at, updated_at, debt_osd, debt_osk, out_balance, in_balance) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                ),
                (created_ts, created_ts, debt_osd, debt_osk, out_balance, in_balance)
            )
    except Exception as e:
        logger.error("[{}] Error while uploading cards stats: {}".format(OP, str(e)))
        raise e

    conn.commit()
    return True


def clean_cards_table() -> Exception | str:
    OP = "repository.clean_cards_table"

    logger.info("[{}] Cleaning cards table".format(OP))

    conn = get_connection()
    cursor = get_cursor()

    try:
        cursor.execute(
            sql.SQL("DELETE FROM card_details")
        )
        conn.commit()
    except Exception as e:
        logger.error("[{}] Error while cleaning cards table: {}".format(OP, str(e)))
        raise e

    logger.info("[{}] Cards table cleaned successfully".format(OP))
    return "Successfully cleaned cards table"


def upload_card_sales(cursor, month: int, year: int, card, worker_id: int) -> Exception | bool:
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
                SET deb_osd = deb_osd + %s,
                    deb_osk = deb_osk + %s,
                    in_balance = in_balance + %s,
                    out_balance = out_balance + %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    card.debt_osd,
                    card.debt_osk,
                    card.in_balance,
                    card.out_balance,
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
                    deb_osd,
                    deb_osk,
                    in_balance,
                    out_balance,
                    worker_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    created_ts,
                    created_ts,
                    card.debt_osd,
                    card.debt_osk,
                    card.in_balance,
                    card.out_balance,
                    worker_id
                )
            )
    except Exception as e:
        logger.error(f"Error in upload_card_sales: {e}")
        raise e

    return True
