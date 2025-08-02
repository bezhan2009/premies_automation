import grpc
from google.protobuf.empty_pb2 import Empty

from gen.python.application import (application_pb2_grpc, application_pb2)


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = application_pb2_grpc.ApplicationServiceStub(channel)

    response_upload = stub.CreateXLSXApplications(application_pb2.CreateXLSXApplicationRequest(
        applications_ids=[1, 2]
    ))
    print(response_upload.xlsx_path)


if __name__ == '__main__':
    run()
