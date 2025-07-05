import grpc

from gen.python.accountant import accountant_pb2, accountant_pb2_grpc
from internal.service import accountant


class AccountantsService(accountant_pb2_grpc.AccountantsServiceServicer):
    def CreateXLSXAccountant(self, request, context):
        try:
            print("=== gRPC CreateXLSXAccountant STARTED ===")
            result = accountant.create_report_for_accountant()
            return accountant_pb2.CreateXLSXAccountantsResponse(xlsx_path=str(result))
        except Exception as e:
            context.abort(grpc.StatusCode.INTERNAL, f"Something went wrong: {str(e)}")
        except FileNotFoundError as ef:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Temporary file not found or file is corrupted: {str(ef)}")
