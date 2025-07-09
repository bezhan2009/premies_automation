import os
import time
from typing import List

import openpyxl
from psycopg2 import sql

from configs.load_configs import get_config
from internal.app.models.accountant import Accountant
from internal.lib.date import get_current_date
from internal.lib.file import get_writable_cell_ref, sanitize_filename
from internal.service.automation.base_automation import BaseAutomation
from internal.sql.reports import (
    count_user_cards_turnover_out_balance_debt_osd,
    count_service_qualities_balls,
    count_mobile_bank_perms,
    count_overdraft_perm,
    get_worker_data,
    get_role_id_by_worker_id,
    count_activated_card_sales
)
from internal.lib.perm_calc import calculate_bonus
from pkg.errors.not_found_error import NotFoundError
from pkg.errors.undefined_role_error import UndefinedRoleError
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


class AccountantAutomation(BaseAutomation):
    def __init__(self):
        self.automation_configs = get_config().automation_details
        self.file_path = ""
        super().__init__("service.automation.AccountantAutomation")

    def _get_accountant_data(self) -> List[Accountant]:
        accountant_data = []
        for owner in self.owners:
            accountant = self._collect_accountant_data(owner)
            accountant_data.append(accountant)
        return accountant_data

    def _collect_accountant_data(self, owner) -> Accountant:
        owner_id, owner_name = owner

        def safe_value(value, default):
            if value is None:
                return default
            return value

        worker_data = self._get_worker_data(owner_id)
        worker_data = (
            safe_value(worker_data[0], 0),
            safe_value(worker_data[1], ''),
            safe_value(worker_data[2], 0),
            safe_value(worker_data[3], 0),
            safe_value(worker_data[4], 0),
            safe_value(worker_data[5], ''),
        )

        turnover_data = self._get_turnover_out_balance_debt_osd(owner_name)
        turnover_data = tuple(safe_value(item, 0) for item in turnover_data)

        service_qualities = self._get_service_qualities_balls(worker_data[0])
        service_qualities = [safe_value(val, 0) for val in service_qualities]

        mobile_bank = self._get_mobile_bank_perms(worker_data[0])
        mobile_bank = [safe_value(val, 0.0) for val in mobile_bank]

        overdraft = self._get_overdraft_perm(worker_data[0])
        overdraft = [safe_value(val, 0.0) for val in overdraft]

        activated_cards = self._get_activated_card_perms(worker_data[0])
        activated_cards = tuple(safe_value(val, 0) for val in activated_cards)

        worker_role_id = self._get_position_from_role_id(worker_data[0])
        worker_role_id = safe_value(worker_role_id, 0)

        if worker_role_id is not None:
            if worker_role_id == 6:
                return Accountant(
                    name=owner_name,
                    salary=worker_data[2],
                    bonus=calculate_bonus(
                        (
                                float(turnover_data[2]) +
                                float(mobile_bank[0]) +
                                float(activated_cards[0])), service_qualities[2], service_qualities[1]),
                )
            elif worker_role_id == 8:
                return Accountant(
                    name=owner_name,
                    salary=worker_data[2],
                    bonus=calculate_bonus(
                        (
                                float(turnover_data[3]) +
                                float(mobile_bank[0]) +
                                float(activated_cards[0])), service_qualities[2], service_qualities[1]))
            else:
                logger.error("Role ID must be 6 or 8")
                raise UndefinedRoleError("Role ID must be 6 or 8")

        logger.error("Role ID not found")
        raise UndefinedRoleError("Role ID not found")

    def _get_turnover_out_balance_debt_osd(self, owner_name: str):
        self.cursor.execute(count_user_cards_turnover_out_balance_debt_osd, {"owner_name": owner_name,
                                                                             "year": self.year,
                                                                             "month": self.month})
        res = self.cursor.fetchone()
        if res is None:
            return [0.0, 0.0, 0.0]
        return res

    def _get_service_qualities_balls(self, owner_id: int):
        self.cursor.execute(count_service_qualities_balls, {"owner_id": owner_id,
                                                            "year": self.year,
                                                            "month": self.month})
        res = self.cursor.fetchone()
        if res is None:
            return [0, 0.0, 0.0]
        return res

    def _get_mobile_bank_perms(self, owner_id: int):
        self.cursor.execute(count_mobile_bank_perms, {"owner_id": owner_id,
                                                      "year": self.year,
                                                      "month": self.month})
        res = self.cursor.fetchone()
        if res is None:
            return [0.0]
        return res

    def _get_overdraft_perm(self, owner_id: int):
        self.cursor.execute(sql.SQL(count_overdraft_perm), {"owner_id": owner_id,
                                                            "year": self.year,
                                                            "month": self.month})
        res = self.cursor.fetchone()
        if res is None:
            return [0.0]
        return res

    def _get_worker_data(self, owner_id: int):
        self.cursor.execute(get_worker_data, {"owner_id": owner_id})
        return self.cursor.fetchone()

    def _get_activated_card_perms(self, owner_id: int):
        self.cursor.execute(count_activated_card_sales, {"owner_id": owner_id})
        res = self.cursor.fetchone()
        if res is None:
            return [0.0]
        return res

    def _get_position_from_role_id(self, owner_id: int) -> int:
        self.cursor.execute(get_role_id_by_worker_id, {"owner_id": owner_id})
        res = self.cursor.fetchone()
        if res is None:
            raise NotFoundError("User/worker not found")

        role_id = res[0]

        return role_id

    def _create_accountant_excel(self, accountants: List[Accountant]) -> str:
        try:
            wb = openpyxl.load_workbook(self.automation_configs.def_excel_paths.def_report_template_accountant)
            ws = wb.active
        except FileNotFoundError as ef:
            raise ef

        data_mapping = self._data_mapping_accountant(accountants)

        for cell_ref, value in data_mapping.items():
            writable_ref = get_writable_cell_ref(ws, cell_ref)
            ws[writable_ref].value = value

        timestamp = int(time.time())
        sanitized_name = sanitize_filename(f"accountant_{timestamp}")
        date_dir = os.path.join(
            self.automation_configs.def_out_paths.accountant_dir_reports,
            get_current_date()
        )

        os.makedirs(date_dir, exist_ok=True)

        output_filename = os.path.join(
            date_dir,
            f"{sanitized_name}.xlsx"
        )

        wb.save(output_filename)
        return output_filename

    def _data_mapping_accountant(self, accountants: List[Accountant]) -> dict:
        data_mapping = {
            "A2": "Месяц: " + get_current_date(),
        }

        cnt = 5

        for owner in accountants:
            data_mapping["A" + str(cnt)] = owner.name
            data_mapping["B" + str(cnt)] = owner.salary
            data_mapping["C" + str(cnt)] = owner.bonus
            cnt += 1

        return data_mapping

    def _create_excel_accountants(self, accountant_data: List[Accountant]):
        self.file_path = output_file = self._create_accountant_excel(accountant_data)
        logger.info(f"Generated Excel file for accountants: {output_file}")

    def create_reports_xlsx(self) -> str:
        try:
            accountant_data = self._get_accountant_data()
            self._create_excel_accountants(accountant_data)
        except NotFoundError as nt:
            raise nt
        except UndefinedRoleError as unr:
            raise unr
        except Exception as e:
            logger.error("Error while creating xlsx file: {}".format(str(e)))
            raise e

        return self.file_path
