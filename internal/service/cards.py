import pandas as pd

from internal.repository import cards
from internal.service.automation.cards_automation import AutomationCard
from internal.service.card_prices import get_coast_dict
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def upload_cards(file_path: str) -> Exception | str:
    OP = "service.upload_cards"

    try:
        df = pd.read_excel(file_path, engine='openpyxl')
    except FileNotFoundError as e:
        logger.error("[{}] File not found {}".format(OP, file_path))
        return e

    clean_cards_table()

    resp = cards.upload_cards(df, get_coast_dict())
    if resp.count("Successfully") == 0:
        return Exception("Something went wrong")

    automation = AutomationCard()

    if automation.set_workers_cards_prem() is not True:
        logger.error("[{}] Error setting workers card prem please check xlsx file".format(OP))
        return Exception("Error setting workers card prem please check your xlsx file")

    if automation.set_workers_turnover_and_activation_prems() is not True:
        logger.error("[{}] Error setting turnovers workers please check your xlsx file".format(OP))
        return Exception("Error setting turnovers workers please check your xlsx file")

    logger.info("[{}] Cards uploaded".format(OP))

    return resp


def clean_cards_table() -> Exception | str:
    return cards.clean_cards_table()
