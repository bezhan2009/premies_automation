import grpc
from gen.python.upload_file import (upload_file_pb2_grpc, upload_file_pb2)


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = upload_file_pb2_grpc.UploadFileServiceStub(channel)

    with open("test.txt", "rb") as f:
        file_bytes = f.read()

    request = upload_file_pb2.UploadFileRequest(
        filename="test.txt",
        file_content=file_bytes
    )

    response = stub.UploadFile(request)
    print("Файл сохранён по пути:", response.path)

    path = response.path

    request = upload_file_pb2.DownloadFileRequest(path=path)
    try:
        response = stub.DownloadFile(request)
        with open("downloaded.txt", "wb") as f:
            f.write(response.file_content)
        print(f"Файл скачан и сохранён как downloaded.txt")
    except grpc.RpcError as e:
        print(f"Ошибка при скачивании: {e.code()}: {e.details()}")


if __name__ == "__main__":
    run()
