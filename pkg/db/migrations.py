from pkg.db.connect import (get_connection, get_cursor)


def migrate():
    conn = get_connection()
    cursor = get_cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS cards (
                id              SERIAL PRIMARY KEY,
                expire_date     DATE,
                issue_date      DATE,
                card_type       TEXT,
                in_balance      NUMERIC,
                debt_osd        NUMERIC,
                debt_osk        NUMERIC,
                out_balance     NUMERIC,
                owner_name      TEXT,
                coast           NUMERIC
            );
            """
        )

        cursor.execute(
            """      
            CREATE TABLE IF NOT EXISTS mobile_bank (
                id SERIAL PRIMARY KEY,
                surname TEXT,
                inn TEXT
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tus_marks (
                id SERIAL,
                dvid DATE,
                req_date DATE,
                code TEXT,
                tus_code TEXT,
                mark INTEGER
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS card_prices (
                id SERIAL,
                dcl_name TEXT,
                coast float
            );
            """
        )

        conn.commit()

        return True
    except Exception as e:
        print(e)
        return False
