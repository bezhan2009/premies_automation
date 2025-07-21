from psycopg2 import sql

from configs.load_configs import get_config
from internal.lib.encypter import hash_sha256
from internal.repository.utils.utils import upsert_mobile_bank_sales
from internal.repository.utils.mb_details import upload_mb_details
from internal.service.automation.base_automation import BaseAutomation
from internal.sql.general import get_worker_id_by_owner_name
from internal.sql.mobile_bank import (
    count_mobile_bank_perms,
    get_mobile_bank_data
)
from internal.app.models.mobile_bank import MobileBank
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


class AutomationMobileBank(BaseAutomation):
    def __init__(self):
        super().__init__("service.automation.AutomationMobileBank", use_month=False, use_year=False)

    def _get_mobile_bank_details(self, worker_name: str, values: dict[str, str]) -> list[MobileBank]:
        self.cursor.execute(
            sql.SQL(get_mobile_bank_data),
            values
        )

        res = self.cursor.fetchall()

        mobile_banks = []

        for row in res:
            mobile_banks.append(
                MobileBank(
                    connects=row[0],
                    prem=self.configs.service.mobile_bank_prem,
                    owner_name=worker_name
                )
            )

        return mobile_banks

    def _set_mobile_bank_details(self, month: int, year: int, worker_id: int, mb_details: list[MobileBank]) -> bool:
        try:
            for mb in mb_details:
                upload_mb_details(self.cursor, month, year, worker_id, mb)
        except Exception as e:
            logger.error("Error in _set_mobile_bank_details: {}".format(e))
            return False

        return True

    def set_mobile_bank_sales(self, month: int, year: int):
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
                        "owner_name": owner[1],
                    }
                )

                worker_id = self.cursor.fetchone()

                upsert_mobile_bank_sales(
                    self.cursor,
                    month,
                    year,
                    mobile_bank_sales[0],
                    mobile_bank_sales[1],
                    worker_id[0]
                )
                self._set_mobile_bank_details(month, year, worker_id[0], self._get_mobile_bank_details(owner[1], values))
            except Exception as e:
                logger.error("[{}] Error while setting mobile bank sales: {}".format(OP, e))
                return False

        self.conn.commit()

        logger.info("[{}] Successfully uploaded mobile bank data".format(OP))

        return True
