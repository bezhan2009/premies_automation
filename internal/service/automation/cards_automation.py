from datetime import date

from psycopg2 import sql
from decimal import Decimal
from internal.lib.encypter import (hash_sha256)
from internal.repository.utils.utils import (
    upsert_card_sales,
    upsert_card_turnovers,
    upsert_card_details
)
from internal.service.automation.base_automation import BaseAutomation
from internal.sql.cards_automation import (
    count_workers_prem_query,
    count_turnovers_and_activation_cards_worker,
    count_workers_cards_sailed_in_general,
    count_workers_cards_sailed,
    count_workers_prem_query_dates,
    get_cards_detail
)
from internal.app.models.card import Card
from internal.sql.general import get_worker_id_by_owner_name
from pkg.logger.logger import setup_logger
from pkg.errors.not_found_error import NotFoundError


logger = setup_logger(__name__)


class AutomationCard(BaseAutomation):
    def __init__(self):
        super().__init__("service.automation.AutomationCard")

    def _set_cards_details(self, month: int, year: int, owner_name: str, owner_id: int) -> bool:
        OP = self.OP + "._set_cards_details"

        self.cursor.execute(
            sql.SQL(get_cards_detail),
            {
                "owner_name": hash_sha256(owner_name),
            }
        )

        data_cards = self.cursor.fetchall()

        card_details = []

        for data_card in data_cards:
            card_detail = Card(
                expire_date=data_card[0],
                issue_date=data_card[1],
                card_type=data_card[2],
                code=data_card[3],
                in_balance=data_card[4],
                debt_osd=data_card[5],
                debt_osk=data_card[6],
                out_balance=data_card[7],
                owner_name=data_card[8],
                coast_cards=data_card[9],
                coast_credits=data_card[10],
                cards_sailed_in_general=int()
            )

            card_details.append(card_detail)
        for card_detail in card_details:
            try:
                upsert_card_details(self.cursor, month, year, card_detail, owner_id, hash_sha256(owner_name))
            except Exception as e:
                logger.error("{} Failed to insert card details: {}".format(OP, e))
                return False

        return True

    def set_workers_cards_prem(self, month: int, year: int) -> bool:
        OP = self.OP + ".set_workers_cards_prem"

        for owner_id, owner_name in self.owners:
            try:
                values = {
                    "owner_name": hash_sha256(owner_name),
                    "year": year,
                    "month": month,
                }

                self.cursor.execute(
                    sql.SQL(count_workers_prem_query),
                    values
                )
                prems = self.cursor.fetchone()

                if prems is None:
                    continue

                self.cursor.execute(
                    sql.SQL(count_workers_prem_query_dates),
                    values
                )
                prems_dates = self.cursor.fetchone()

                if prems is None:
                    continue

                self.cursor.execute(
                    sql.SQL(count_workers_cards_sailed_in_general),
                    values
                )

                cards_sailed_in_general = self.cursor.fetchone()

                self.cursor.execute(
                    sql.SQL(count_workers_cards_sailed),
                    values
                )

                cards_sailed = self.cursor.fetchone()

                self.cursor.execute(
                    sql.SQL(get_worker_id_by_owner_name),
                    {
                        "owner_name": owner_name,
                    }
                )

                worker_id = self.cursor.fetchone()

                if not worker_id:
                    logger.error("[{}] Error while setting card prems: worker not found".format(OP))
                    continue

                card = Card(
                    debt_osd=prems[3],
                    debt_osk=prems[4],
                    in_balance=prems[5],
                    out_balance=prems[6],
                    cards_sailed_in_general=cards_sailed_in_general[0],

                    expire_date=date(year, month, 1),
                    issue_date=date(year, month, 1),
                    card_type=str(),
                    code=str(),
                    owner_name=str(),
                    coast_cards=float(),
                    coast_credits=float()
                )

                if worker_id[1] == 6:
                    if upsert_card_sales(self.cursor, month, year, cards_sailed[0], prems_dates[1], card, worker_id[0]) is False:
                        return False
                elif worker_id[1] == 8:
                    if upsert_card_sales(self.cursor, month, year, cards_sailed[0], prems_dates[2], card, worker_id[0]) is False:
                        return False
                else:
                    logger.error("[{}] Error while setting card prems: worker role id not found".format(OP))
                    raise NotFoundError("worker role id not found")
            except Exception as e:
                logger.error("[{}] Error while setting card prems: {}".format(OP, str(e)))
                return False

        self.conn.commit()
        logger.info(f"[{OP}] All workers updated card sales prems.")
        return True

    def set_workers_turnover_and_activation_prems(self, month: int, year: int) -> bool:
        OP = self.OP + ".set_workers_turnover_and_activation_prems"

        for owner_id, owner_name in self.owners:
            try:
                self.cursor.execute(
                    sql.SQL(count_turnovers_and_activation_cards_worker),
                    {
                        "owner_name": hash_sha256(owner_name),
                    }
                )

                cards_turnover = self.cursor.fetchone()
                if cards_turnover is None or cards_turnover[0] is None:
                    continue

                self.cursor.execute(
                    sql.SQL(get_worker_id_by_owner_name),
                    {
                        "owner_name": owner_name,
                    }
                )

                worker_id = self.cursor.fetchone()
                if worker_id is None:
                    continue

                if upsert_card_turnovers(self.cursor, month, year, cards_turnover[0] / Decimal('0.8'),
                                         cards_turnover[0],
                                         cards_turnover[1], worker_id[0]) is False:
                    return False
            except Exception as e:
                logger.error(f"[{OP}] Error for {owner_name}: {e}")
                return False

        self.conn.commit()
        logger.info(f"[{OP}] All workers updated turnovers.")

        return True
