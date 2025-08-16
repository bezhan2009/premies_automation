from typing import List

import grpc

from gen.python.poll import poll_pb2, poll_pb2_grpc
from internal.service import poll


class PollService(poll_pb2_grpc.PollServiceServicer):
    def CreateDOCXPoll(self, request, context):
        try:
            print("=== gRPC CreateDOCXPoll STARTED ===")
            result = poll.create_poll(request.applications_ids)
            return poll_pb2.CreateDOCXPollResponse(zip_docx_path=str(result))
        except Exception as e:
            context.abort(grpc.StatusCode.INTERNAL, f"Something went wrong: {str(e)}")
        except FileNotFoundError as ef:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Temporary file not found or file is corrupted: {str(ef)}")
