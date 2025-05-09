import psycopg2

from configs.load_configs import get_config

# приватные переменные
_connection = None
_cursor = None


def connect_to_db():
    global _connection, _cursor
    configs = get_config()

    _connection = psycopg2.connect(
        dbname=configs.database.name,
        user=configs.database.user,
        password=configs.database.password,
        host=configs.database.host,
        port=configs.database.port
    )

    _cursor = _connection.cursor()


def close_db_connection():
    global _connection
    if _connection:
        _connection.close()
        _connection = None


def close_db_cursor():
    global _cursor
    if _cursor:
        _cursor.close()
        _cursor = None


def get_connection():
    global _connection
    return _connection


def get_cursor():
    global _cursor
    return _cursor
