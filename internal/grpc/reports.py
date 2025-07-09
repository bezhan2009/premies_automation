import grpc

from gen.python.reports import reports_pb2, reports_pb2_grpc
from internal.service import reports


class ReportsServiceServicer(reports_pb2_grpc.ReportsServiceServicer):
    def CreateZIPReports(self, request, context):
        try:
            print("=== gRPC CreateZIPReports STARTED ===")
            result = reports.create_zip_reports(request.month, request.year)
            return reports_pb2.CreateZIPReportsResponse(zip_path=str(result))
        except Exception as e:
            context.abort(grpc.StatusCode.INTERNAL, f"Something went wrong: {str(e)}")
        except FileNotFoundError as ef:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Temporary file not found: {str(ef)}")

    def CreateExcelReport(self, request, context):
        try:
            print("=== gRPC CreateExcelReport STARTED ===")
            result = reports.create_zip_report_one_employee(request.owner_id, request.month, request.year)
            return reports_pb2.CreateExcelReportResponse(zip_path=str(result))
        except Exception as e:
            context.abort(grpc.StatusCode.INTERNAL, f"Something went wrong: {str(e)}")
        except FileNotFoundError as ef:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Temporary file not found: {str(ef)}")
