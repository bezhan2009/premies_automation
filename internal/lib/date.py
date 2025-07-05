from datetime import datetime, timezone


def get_current_month():
    return datetime.now().month


def get_current_year():
    return datetime.now().year


def get_current_date():
    today = datetime.now()

    # Словарь для преобразования номера месяца в название в родительном падеже
    months = {
        1: "Января",
        2: "Февраля",
        3: "Марта",
        4: "Апреля",
        5: "Мая",
        6: "Июня",
        7: "Июля",
        8: "Августа",
        9: "Сентября",
        10: "Октября",
        11: "Ноября",
        12: "Декабря"
    }

    month_name = months.get(today.month, "")
    year = today.year

    return f"{month_name} {year}"


def get_month_date_range():
    year, month = get_current_year(), get_current_month()
    start_date = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)

    return start_date, end_date
