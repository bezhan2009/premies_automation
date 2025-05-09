from concurrent import futures

import grpc

from internal.app.grpc.services import add_services
from internal.app.grpc.start import start_application
from internal.app.models.configs import Config
from pkg.logger.logger import (setup_logger)

logger = setup_logger(__name__)


def serve(config: Config):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=config.grpc.max_workers))
    add_services(server)

    server.add_insecure_port('[::]:{}'.format(config.grpc.port))

    start_application(server)
