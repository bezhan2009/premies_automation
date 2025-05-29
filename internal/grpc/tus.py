import grpc

from gen.python.tus import tus_pb2, tus_pb2_grpc
from internal.service import tus


class TusServiceServicer(tus_pb2_grpc.TusServiceServicer):
    def UploadTusData(self, request, context):
        try:
            result = tus.tus_excel_upload(request.file_path)
            return tus_pb2.TusUploadResponse(status=str(result))
        except Exception as e:
            context.abort(grpc.StatusCode.NOT_FOUND, f"No such file or directory: {str(e)}")

    def CleanTusTable(self, request, context):
        try:
            result = tus.tus_clean_table()
            return tus_pb2.TusCleanResponse(status=str(result))
        except Exception as e:
            context.abort(grpc.StatusCode.INTERNAL, f"Something went wrong: {str(e)}")
