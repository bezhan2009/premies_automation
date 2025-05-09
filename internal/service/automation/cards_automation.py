from psycopg2 import sql

from internal.lib.date import (get_current_month, get_current_year)
from internal.lib.encypter import (hash_sha256)
from internal.repository.utils.utils import (
    get_users,
    insert_card_sales,
    insert_card_turnovers
)
from internal.sql.cards_automation import (
    count_workers_prem_query,
    count_workers_card_turnover_query
)
from pkg.db.connect import (get_connection, get_cursor)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


class AutomationCard:
    def __init__(self):
        self.OP = "service.automation.AutomationCard"
        self.conn = get_connection()
        self.cursor = get_cursor()
        self.month = get_current_month()
        self.year = get_current_year()
        self.owners = self._fetch_owners()

    def _fetch_owners(self):
        try:
            return get_users(self.cursor)
        except Exception as e:
            logger.error(f"[{self.OP}] Error while selecting users: {e}")
            return []

    def set_workers_cards_prem(self) -> bool:
        OP = self.OP + ".set_workers_prem"

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

                if insert_card_sales(self.cursor, prems[2], 0, owner_id) is False:
                    return False
            except Exception as e:
                logger.error("[{}] Error while setting card prems: {}".format(OP, str(e)))
                return False

        self.conn.commit()
        logger.info(f"[{OP}] All workers updated.")
        return True

    def set_workers_turnover(self) -> bool:
        OP = self.OP + ".set_workers_turnover"

        for owner_id, owner_name in self.owners:
            try:
                self.cursor.execute(
                    sql.SQL(count_workers_card_turnover_query),
                    {
                        "owner_name": hash_sha256(owner_name),
                    }
                )

                cards_turnover = self.cursor.fetchone()
                if cards_turnover is None or cards_turnover[0] is None:
                    continue

                if insert_card_turnovers(self.cursor, cards_turnover[0], owner_id) is False:
                    return False
            except Exception as e:
                logger.error(f"[{OP}] Error for {owner_name}: {e}")
                return False

        self.conn.commit()
        logger.info(f"[{OP}] All workers updated turnovers.")

        return True
