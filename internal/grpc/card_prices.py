from gen.python.card_prices import card_prices_pb2, card_prices_pb2_grpc
from internal.service import card_prices


class CardPricesServiceServicer(card_prices_pb2_grpc.CardPricesServiceServicer):
    def UploadCardPricesData(self, request, context):
        try:
            result = card_prices.upload_card_prices(request.file_path)
            return card_prices_pb2.CardPricesUploadResponse(status=result)
        except Exception as e:
            return card_prices_pb2.CardPricesUploadResponse(status=f"Error: {str(e)}")
