import os

from dotenv import load_dotenv

from configs.load_configs import load_config, get_config
from internal.app.grpc.app import serve
from pkg.db.connect import connect_to_db
from pkg.db.migrations import migrate
from pkg.logger.logger import setup_logger
from pkg.utils.init_file_paths import ensure_directories_exist


logger = setup_logger(__name__)


def main():
    load_dotenv(".env")

    load_config(os.getenv("CONFIGS_PATH"))

    if not os.path.exists("uploads"):
        os.makedirs("uploads")

    config = get_config()

    connect_to_db()

    ensure_directories_exist()

    if not migrate():
        logger.critical("Migration failed")
        raise Exception("Migration failed")

    serve(config)


if __name__ == "__main__":
    main()
