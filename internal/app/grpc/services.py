from gen.python.card_prices import card_prices_pb2_grpc
from gen.python.cards import cards_pb2_grpc
from gen.python.mobile_bank import mobile_bank_pb2_grpc
from gen.python.reports import get_worker_reports_pb2_grpc
from gen.python.tus import tus_pb2_grpc
from internal.grpc.card_prices import CardPricesServiceServicer
from internal.grpc.cards import CardsServiceServicer
from internal.grpc.mobile_bank import MobileBankServiceServicer
from internal.grpc.reports import ReportsServiceServicer
from internal.grpc.tus import TusServiceServicer
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def add_services(server):
    logger.info("Adding TusServiceServicer...")
    tus_pb2_grpc.add_TusServiceServicer_to_server(TusServiceServicer(), server)

    logger.info("Adding MobileBankServiceServicer...")
    mobile_bank_pb2_grpc.add_MobileBankServiceServicer_to_server(MobileBankServiceServicer(), server)

    logger.info("Adding CardPriceServiceServicer...")
    card_prices_pb2_grpc.add_CardPricesServiceServicer_to_server(CardPricesServiceServicer(), server)

    logger.info("Adding CardsServiceServicer...")
    cards_pb2_grpc.add_CardsServiceServicer_to_server(CardsServiceServicer(), server)

    logger.info("Adding ReportsServiceServicer...")
    get_worker_reports_pb2_grpc.add_ReportsServiceServicer_to_server(ReportsServiceServicer(), server)
