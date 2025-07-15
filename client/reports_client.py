import grpc
from google.protobuf.empty_pb2 import Empty

from gen.python.reports import (reports_pb2, reports_pb2_grpc)


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = reports_pb2_grpc.ReportsServiceStub(channel)

    # response_upload = stub.CreateZIPReports(reports_pb2.CreateZIPReportsRequest(
    #     month=6,
    #     year=2025
    # ))
    # print(response_upload.zip_path)

    response_upload = stub.CreateExcelReport(reports_pb2.CreateExcelReportRequest(
        owner_id=55,
        month=6,
        year=2025,
    ))

    print(response_upload.zip_path)


if __name__ == '__main__':
    run()
