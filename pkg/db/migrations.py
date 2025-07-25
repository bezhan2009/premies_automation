from pkg.db.connect import (get_connection, get_cursor)


def migrate():
    conn = get_connection()
    cursor = get_cursor()

    try:
        cursor.execute(
            """
            CREATE EXTENSION IF NOT EXISTS pg_trgm;
            """
        )

        cursor.execute(
            """      
            CREATE TABLE IF NOT EXISTS mobile_bank (
                id SERIAL PRIMARY KEY,
                surname TEXT,
                mobile_bank_connects INT
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tus_marks (
                id SERIAL,
                surname TEXT,
                mark FLOAT
            );
            """
        )

        # Добавление колонки tests, если её нет
        cursor.execute(
            """
            ALTER TABLE tus_marks
            ADD COLUMN IF NOT EXISTS tests FLOAT;
            """
        )

        # Добавление колонки complaints, если её нет
        cursor.execute(
            """
            ALTER TABLE tus_marks
            ADD COLUMN IF NOT EXISTS complaints INTEGER;
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS card_prices (
                id SERIAL,
                dcl_name TEXT,
                coast_cards float,
                coast_credits float,
                category TEXT
            );
            """
        )

        conn.commit()

        return True
    except Exception as e:
        print(e)
        return False
