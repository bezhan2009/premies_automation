import grpc

from gen.python.mobile_bank import (mobile_bank_pb2, mobile_bank_pb2_grpc)


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = mobile_bank_pb2_grpc.MobileBankServiceStub(channel)

    response_upload = stub.UploadMobileBankData(
        mobile_bank_pb2.MobileBankUploadRequest(file_path="./uploads/mobile_bank.xlsx"))
    print(response_upload.status)

    # response_clean = stub.CleanMobileBankTable(Empty())
    # print(response_clean.status)


if __name__ == '__main__':
    run()
