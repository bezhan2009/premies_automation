from psycopg2 import sql

from internal.lib.encypter import hash_sha256
from internal.repository.utils.utils import get_users
from internal.repository.utils.utils import insert_mobile_bank_sales
from internal.sql.mobile_bank import count_mobile_bank_perms
from pkg.db.connect import (get_connection, get_cursor)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


class AutomationMobileBank:
    def __init__(self):
        self.OP = "service.automation.AutomationMobileBank"
        self.conn = get_connection()
        self.cursor = get_cursor()
        self.owners = self._fetch_owners()

    def _fetch_owners(self):
        try:
            return get_users(self.cursor)
        except Exception as e:
            logger.error(f"[{self.OP}] Error while selecting users: {e}")
            return []

    def set_mobile_bank_sales(self):
        OP = self.OP + ".set_mobile_bank_sales"

        for owner in self.owners:
            try:
                values = {
                    "surname": hash_sha256(owner[1]),
                }

                self.cursor.execute(
                    sql.SQL(count_mobile_bank_perms),
                    values
                )

                mobile_bank_sales = self.cursor.fetchone()

                if mobile_bank_sales is None:
                    continue

                insert_mobile_bank_sales(self.cursor, mobile_bank_sales[0], owner[0])
            except Exception as e:
                logger.error("[{}] Error while setting mobile bank sales: {}".format(OP, e))
                return False

        self.conn.commit()

        return True
