from gen.python.cards import cards_pb2, cards_pb2_grpc
from internal.service import cards


class CardsServiceServicer(cards_pb2_grpc.CardsServiceServicer):
    def UploadCardsData(self, request, context):
        try:
            result = cards.upload_cards(request.file_path)
            return cards_pb2.CardsUploadResponse(status=result)
        except Exception as e:
            return cards_pb2.CardsUploadResponse(status=f"Error: {str(e)}")

    def CleanCardsTable(self, request, context):
        try:
            result = cards.clean_cards_table()
            return cards_pb2.CardsCleanResponse(status=result)
        except Exception as e:
            return cards_pb2.CardsCleanResponse(status=f"Error: {str(e)}")
