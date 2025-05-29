import grpc

from gen.python.card_prices import card_prices_pb2, card_prices_pb2_grpc
from internal.service import card_prices


class CardPricesServiceServicer(card_prices_pb2_grpc.CardPricesServiceServicer):
    def UploadCardPricesData(self, request, context):
        try:
            result = card_prices.upload_card_prices(request.file_path)
            return card_prices_pb2.CardPricesUploadResponse(status=str(result))
        except Exception as e:
            context.abort(grpc.StatusCode.NOT_FOUND, f"No such file or directory: {str(e)}")
