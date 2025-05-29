import grpc
from google.protobuf.empty_pb2 import Empty

from gen.python.reports import (get_worker_reports_pb2_grpc)


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = get_worker_reports_pb2_grpc.ReportsServiceStub(channel)

    response_upload = stub.CreateZIPReports(Empty())
    print(response_upload.zip_path)


if __name__ == '__main__':
    run()
