import grpc
from google.protobuf.empty_pb2 import Empty

from gen.python.accountant import (accountant_pb2_grpc, accountant_pb2)


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = accountant_pb2_grpc.AccountantsServiceStub(channel)

    response_upload = stub.CreateXLSXAccountant(accountant_pb2.CreateXLSXAccountantsRequest(
        month=6, year=2025
    ))
    print(response_upload.xlsx_path)


if __name__ == '__main__':
    run()
