import grpc

from gen.python.cards import cards_pb2, cards_pb2_grpc
from internal.service import cards


class CardsServiceServicer(cards_pb2_grpc.CardsServiceServicer):
    def UploadCardsData(self, request, context):
        try:
            result = cards.upload_cards(request.file_path)
            return cards_pb2.CardsUploadResponse(status=str(result))
        except Exception as e:
            context.abort(grpc.StatusCode.NOT_FOUND, f"No such file or directory: {str(e)}")

    def CleanCardsTable(self, request, context):
        try:
            result = cards.clean_cards_table()
            return cards_pb2.CardsCleanResponse(status=str(result))
        except Exception as e:
            context.abort(grpc.StatusCode.INTERNAL, f"Error: {str(e)}")
