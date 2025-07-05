import grpc

from gen.python.upload_file import upload_file_pb2_grpc
from gen.python.upload_file import upload_file_pb2
from internal.service.upload_file import (upload_file, download_file)
from pkg.errors.permission_denied_error import PermissionDeniedError
from pkg.errors.not_found_error import NotFoundError


class UploadFileService(upload_file_pb2_grpc.UploadFileServiceServicer):
    def UploadFile(self, request, context):
        try:
            save_path = upload_file(request)
            return upload_file_pb2.UploadFileResponse(path=save_path)
        except Exception as e:
            context.abort(grpc.StatusCode.INTERNAL, "Something went wrong")
            return upload_file_pb2.UploadFileResponse()

    def DownloadFile(self, request, context):
        try:
            content = download_file(request)
            return upload_file_pb2.DownloadFileResponse(file_content=content)
        except PermissionDeniedError as pr:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details("Access denied. Only files inside uploads are allowed.")
            return upload_file_pb2.DownloadFileResponse()
        except NotFoundError as nt:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("File not found.")
            return upload_file_pb2.DownloadFileResponse()
        except Exception as e:
            context.abort(grpc.StatusCode.INTERNAL, "Something went wrong")
            return upload_file_pb2.DownloadFileResponse()
