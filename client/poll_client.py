import grpc
from google.protobuf.empty_pb2 import Empty

from gen.python.poll import (poll_pb2_grpc, poll_pb2)


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = poll_pb2_grpc.PollServiceStub(channel)

    response_upload = stub.CreateDOCXPoll(poll_pb2.CreateDOCXPollRequest(
        applications_ids=[1, 2, 3]
    ))
    print(response_upload.zip_docx_path)


if __name__ == '__main__':
    run()
