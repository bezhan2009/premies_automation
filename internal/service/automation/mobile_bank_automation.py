from contextlib import closing
from psycopg2 import sql
from typing import List, Dict

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

    def _get_mobile_bank_details(self, worker_name: str, values: Dict[str, str]) -> List[MobileBank]:
        """Получает данные о мобильном банке для указанного сотрудника"""
        try:
            with closing(self.conn.cursor()) as cursor:
                cursor.execute(
                    sql.SQL(get_mobile_bank_data),
                    values
                )
                res = cursor.fetchall()

            return [
                MobileBank(
                    connects=row[0],
                    prem=self.configs.service.mobile_bank_prem,
                    owner_name=worker_name
                )
                for row in res
            ]
        except Exception as e:
            logger.error(f"[{self.OP}] Error getting mobile bank details: {str(e)}")
            raise

    def _set_mobile_bank_details(self, month: int, year: int, worker_id: int, mb_details: List[MobileBank]) -> bool:
        """Сохраняет детали мобильного банка для сотрудника"""
        OP = f"{self.OP}._set_mobile_bank_details"

        try:
            with closing(self.conn.cursor()) as cursor:
                for mb in mb_details:
                    upload_mb_details(cursor, month, year, worker_id, mb)
            return True
        except Exception as e:
            logger.error(f"[{OP}] Error setting mobile bank details: {str(e)}")
            self.conn.rollback()
            return False

    def set_mobile_bank_sales(self, month: int, year: int) -> bool:
        """Основной метод для обработки и сохранения данных мобильного банка"""
        OP = f"{self.OP}.set_mobile_bank_sales"

        try:
            with closing(self.conn.cursor()) as cursor:
                for owner in self.owners:
                    try:
                        values = {"surname": hash_sha256(owner[1])}

                        # Получаем данные о продажах
                        cursor.execute(sql.SQL(count_mobile_bank_perms), values)
                        mobile_bank_sales = cursor.fetchone()

                        if mobile_bank_sales is None:
                            continue

                        # Получаем ID сотрудника
                        cursor.execute(
                            sql.SQL(get_worker_id_by_owner_name),
                            {"owner_name": owner[1]}
                        )
                        worker_id = cursor.fetchone()

                        if not worker_id:
                            logger.warning(f"[{OP}] Worker not found: {owner[1]}")
                            continue

                        # Обновляем данные
                        upsert_mobile_bank_sales(
                            cursor,
                            month,
                            year,
                            mobile_bank_sales[0],
                            mobile_bank_sales[1],
                            worker_id[0]
                        )

                        # Сохраняем детали
                        mb_details = self._get_mobile_bank_details(owner[1], values)
                        self._set_mobile_bank_details(month, year, worker_id[0], mb_details)

                    except Exception as e:
                        logger.error(f"[{OP}] Error processing owner {owner[1]}: {str(e)}")
                        continue

            self.conn.commit()
            logger.info(f"[{OP}] Successfully processed mobile bank data")
            return True

        except Exception as e:
            self.conn.rollback()
            logger.error(f"[{OP}] Transaction failed: {str(e)}")
            return False
