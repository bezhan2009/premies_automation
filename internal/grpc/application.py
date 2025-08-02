import grpc

from gen.python.application import application_pb2, application_pb2_grpc
from internal.service import application


class ApplicationService(application_pb2_grpc.ApplicationServiceServicer):
    def CreateXLSXApplications(self, request, context):
        try:
            print("=== gRPC CreateXLSXAccountant STARTED ===")
            result = application.create_xlsx_file_applications(request.applications_ids)
            return application_pb2.CreateXLSXApplicationResponse(xlsx_path=str(result))
        except Exception as e:
            context.abort(grpc.StatusCode.INTERNAL, f"Something went wrong: {str(e)}")
        except FileNotFoundError as ef:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Temporary file not found or file is corrupted: {str(ef)}")
