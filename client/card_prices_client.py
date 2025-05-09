import grpc

from gen.python.card_prices import (card_prices_pb2, card_prices_pb2_grpc)


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = card_prices_pb2_grpc.CardPricesServiceStub(channel)

    response_upload = stub.UploadCardPricesData(
        card_prices_pb2.CardPricesUploadRequest(file_path="./uploads/prices.xlsx"))
    print(response_upload.status)

    # response_clean = stub.CleanMobileBankTable(Empty())
    # print(response_clean.status)


if __name__ == '__main__':
    run()
