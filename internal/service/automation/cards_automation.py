from psycopg2 import sql

from internal.lib.encypter import (hash_sha256)
from internal.repository.utils.utils import (
    upsert_card_sales,
    upsert_card_turnovers
)
from internal.service.automation.base_automation import BaseAutomation
from internal.sql.cards_automation import (
    count_workers_prem_query,
    count_turnovers_and_activation_cards_worker
)
from internal.sql.general import get_worker_id_by_owner_name
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


class AutomationCard(BaseAutomation):
    def __init__(self):
        super().__init__("service.automation.AutomationCard")

    def set_workers_cards_prem(self) -> bool:
        OP = self.OP + ".set_workers_cards_prem"

        for owner_id, owner_name in self.owners:
            try:
                values = {
                    "owner_name": hash_sha256(owner_name),
                    "year": self.year,
                    "month": self.month,
                }
                self.cursor.execute(
                    sql.SQL(count_workers_prem_query),
                    values
                )
                prems = self.cursor.fetchone()

                if prems is None:
                    continue

                self.cursor.execute(
                    sql.SQL(get_worker_id_by_owner_name),
                    {
                        "owner_name": hash_sha256(owner_name),
                    }
                )

                worker_id = self.cursor.fetchone()

                if upsert_card_sales(self.cursor, prems[2], 0, worker_id[0]) is False:
                    return False
            except Exception as e:
                logger.error("[{}] Error while setting card prems: {}".format(OP, str(e)))
                return False

        self.conn.commit()
        logger.info(f"[{OP}] All workers updated.")
        return True

    def set_workers_turnover_and_activation_prems(self) -> bool:
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
                        "owner_name": hash_sha256(owner_name),
                    }
                )

                worker_id = self.cursor.fetchone()

                if upsert_card_turnovers(self.cursor, cards_turnover[0], cards_turnover[1], worker_id[0]) is False:
                    return False
            except Exception as e:
                logger.error(f"[{OP}] Error for {owner_name}: {e}")
                return False

        self.conn.commit()
        logger.info(f"[{OP}] All workers updated turnovers.")

        return True
