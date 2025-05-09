import logging
import os
from logging.handlers import RotatingFileHandler

LOG_DIR = "logs"
MAX_BYTES = 5 * 1024 * 1024  # 5 MB
BACKUP_COUNT = 3  # сколько архивов хранить

LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}


def create_log_dir_and_files():
    os.makedirs(LOG_DIR, exist_ok=True)
    for level in LEVELS:
        path = os.path.join(LOG_DIR, f"{level}.log")
        if not os.path.exists(path):
            open(path, "a").close()


def setup_logger(name: str) -> logging.Logger:
    create_log_dir_and_files()

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Уникальные обработчики для каждого уровня
    for level_name, level_value in LEVELS.items():
        handler = RotatingFileHandler(
            os.path.join(LOG_DIR, f"{level_name}.log"),
            maxBytes=MAX_BYTES,
            backupCount=BACKUP_COUNT,
            encoding='utf-8'
        )
        handler.setLevel(level_value)
        handler.setFormatter(formatter)

        # Добавляем фильтр, чтобы каждый файл логировал только свой уровень
        handler.addFilter(lambda record, lvl=level_value: record.levelno == lvl)
        logger.addHandler(handler)

    # Также лог в консоль (если хочешь)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
