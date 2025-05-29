from psycopg2 import sql

from internal.lib.encypter import hash_sha256
from internal.repository.utils.utils import upsert_mobile_bank_sales
from internal.service.automation.base_automation import BaseAutomation
from internal.sql.general import get_worker_id_by_owner_name
from internal.sql.mobile_bank import count_mobile_bank_perms
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


class AutomationMobileBank(BaseAutomation):
    def __init__(self):
        super().__init__("service.automation.AutomationMobileBank", use_month=False, use_year=False)

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

                self.cursor.execute(
                    sql.SQL(get_worker_id_by_owner_name),
                    {
                        "owner_name": hash_sha256(owner[1]),
                    }
                )

                worker_id = self.cursor.fetchone()

                upsert_mobile_bank_sales(self.cursor, mobile_bank_sales[0], worker_id[0])
            except Exception as e:
                logger.error("[{}] Error while setting mobile bank sales: {}".format(OP, e))
                return False

        self.conn.commit()

        return True
