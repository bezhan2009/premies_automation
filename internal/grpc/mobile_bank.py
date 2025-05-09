from gen.python.mobile_bank import mobile_bank_pb2, mobile_bank_pb2_grpc
from internal.service import mobile_bank


class MobileBankServiceServicer(mobile_bank_pb2_grpc.MobileBankServiceServicer):
    def UploadMobileBankData(self, request, context):
        try:
            result = mobile_bank.mobile_bank_excel_upload(request.file_path)
            return mobile_bank_pb2.MobileBankUploadResponse(status=result)
        except Exception as e:
            return mobile_bank_pb2.MobileBankUploadResponse(status=f"Error: {str(e)}")

    def CleanMobileBankTable(self, request, context):
        try:
            result = mobile_bank.mobile_bank_clean_table()
            return mobile_bank_pb2.MobileBankCleanResponse(status=result)
        except Exception as e:
            return mobile_bank_pb2.MobileBankCleanResponse(status=f"Error: {str(e)}")
