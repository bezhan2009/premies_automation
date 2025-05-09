import grpc

from gen.python.tus import (tus_pb2, tus_pb2_grpc)


# from google.protobuf.empty_pb2 import Empty


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = tus_pb2_grpc.TusServiceStub(channel)

    response_upload = stub.UploadTusData(tus_pb2.TusUploadRequest(file_path="./uploads/tus_marks.xlsx"))
    print(response_upload.status)
    #
    # response_clean = stub.CleanTusTable(Empty())
    # print(response_clean.status)


if __name__ == '__main__':
    run()
