import traceback

from configs.load_configs import get_config
from internal.lib.encypter import init_encryption
from internal.service.card_prices import upload_card_prices_to_dict
from pkg.db.connect import (close_db_connection, close_db_cursor)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def start_application(server):
    init_encryption()

    upload_card_prices_to_dict()

    config = get_config()

    try:
        server.start()
        logger.info("Started grpc server on port {}".format(config.grpc.port))
        server.wait_for_termination()
    except KeyboardInterrupt:
        stop_application(server)
    except Exception as e:
        logger.critical("Error starting grpc server: {}".format(e))
        logger.critical(traceback.format_exc())
        exit(1)


def stop_application(server):
    config = get_config()

    logger.info("Stopping grpc server on port {}".format(config.grpc.port))

    close_db_connection()
    close_db_cursor()

    logger.info("Stopped grpc server on port {}".format(config.grpc.port))
    server.stop(0)
