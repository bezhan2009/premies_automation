import grpc

from gen.python.reports import get_worker_reports_pb2, get_worker_reports_pb2_grpc
from internal.service import reports


class ReportsServiceServicer(get_worker_reports_pb2_grpc.ReportsServiceServicer):
    def CreateZIPReports(self, request, context):
        try:
            print("=== gRPC CreateZIPReports STARTED ===")
            result = reports.create_zip_reports()
            return get_worker_reports_pb2.CreateZIPReportsResponse(zip_path=str(result))
        except Exception as e:
            context.abort(grpc.StatusCode.INTERNAL, f"Something went wrong: {str(e)}")
        except FileNotFoundError as ef:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Temporary file not found: {str(ef)}")
