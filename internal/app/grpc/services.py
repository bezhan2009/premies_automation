from gen.python.card_prices import card_prices_pb2_grpc
from gen.python.cards import cards_pb2_grpc
from gen.python.mobile_bank import mobile_bank_pb2_grpc
from gen.python.reports import get_worker_reports_pb2_grpc
from gen.python.tus import tus_pb2_grpc
from gen.python.upload_file import upload_file_pb2_grpc
from gen.python.accountant import accountant_pb2_grpc
from internal.grpc.card_prices import CardPricesServiceServicer
from internal.grpc.cards import CardsServiceServicer
from internal.grpc.mobile_bank import MobileBankServiceServicer
from internal.grpc.reports import ReportsServiceServicer
from internal.grpc.tus import TusServiceServicer
from internal.grpc.upload_file import UploadFileService
from internal.grpc.accountant import AccountantsService
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

    logger.info("Adding UploadFileServiceServicer...")
    upload_file_pb2_grpc.add_UploadFileServiceServicer_to_server(UploadFileService(), server)

    logger.info("Adding AccountantsServicer...")
    accountant_pb2_grpc.add_AccountantsServiceServicer_to_server(AccountantsService(), server)
