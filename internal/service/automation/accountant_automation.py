import os
import time
from typing import List, Union

import openpyxl
from psycopg2 import sql

from configs.load_configs import get_config
from internal.app.models.accountant import Accountant
from internal.lib.date import get_current_date
from internal.lib.encypter import hash_sha256
from internal.lib.file import get_writable_cell_ref, sanitize_filename
from internal.service.automation.base_automation import BaseAutomation
from internal.sql.cards_automation import count_workers_prem_query_dates
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
        result: List[Accountant] = []
        for owner in self.owners:
            acct = self._collect_accountant_data(owner)
            if acct is not None:
                result.append(acct)
        return result

    def _collect_accountant_data(self, owner) -> Union[Accountant, None]:
        owner_id, owner_name = owner

        def safe_value(val, default: Union[int, float, str]) -> Union[float, str]:
            # Нормализуем None → default, списки/кортежи → первый элемент, всё остальное пытаемся float()
            if val is None:
                return default
            if isinstance(val, (list, tuple)):
                return safe_value(val[0] if val else None, default)
            try:
                # Если default ― строка, значит ожидаем строку
                if isinstance(default, str):
                    return str(val)
                return float(val)
            except (ValueError, TypeError):
                return default

        # 1) Raw worker data
        raw = self._get_worker_data(owner_id)
        if raw is None:
            # Если нет строки с данными работника, пропускаем
            logger.warning(f"No worker data for owner_id={owner_id}")
            return None

        # 2) Преобразуем в кортеж нормализованных значений:
        #    (id:int, name:str, salary:float, field3, field4, field5)
        worker_data = (
            safe_value(raw[0], 0),        # owner_id
            safe_value(raw[1], ''),       # owner_name
            safe_value(raw[2], 0),        # salary
            safe_value(raw[3], 0),        # (unused?)
            safe_value(raw[4], 0),        # (unused?)
            safe_value(raw[5], ''),       # (unused?)
        )

        # 3) Проверяем роль — получаем роль по реальному owner_id (целое число):
        role_raw = self._get_position_from_role_id(int(worker_data[0]))
        if role_raw is None:
            raise UndefinedRoleError(f"Cannot determine role for worker {owner_id}")
        worker_role_id = int(role_raw)

        # 4) Turnover/out/balance/debt/osd
        td = self._get_turnover_out_balance_debt_osd(owner_name)
        turnover_data = tuple(safe_value(x, 0.0) for x in td)

        # 5) Service qualities
        sq = self._get_service_qualities_balls(int(worker_data[0]))
        service_qualities = [safe_value(x, 0.0) for x in sq]

        # 6) Mobile bank perms
        mb = self._get_mobile_bank_perms(int(worker_data[0]))
        mobile_bank = [safe_value(x, 0.0) for x in mb]

        # 7) Overdraft perms
        od = self._get_overdraft_perm(int(worker_data[0]))
        overdraft = [safe_value(x, 0.0) for x in od]

        # 8) Activated card sales
        ac = self._get_activated_card_perms(int(worker_data[0]))
        activated_cards = tuple(safe_value(x, 0.0) for x in ac)

        # 9) Dates for prem
        values = {
            "owner_name": hash_sha256(owner_name),
            "year": self.year,
            "month": self.month,
        }
        self.cursor.execute(sql.SQL(count_workers_prem_query_dates), values)
        pd_raw = self.cursor.fetchone()
        if pd_raw is None:
            # Никогда не было дат премий — пропускаем
            return None
        prems_dates = tuple(safe_value(x, 0.0) for x in pd_raw)

        # 10) Считаем базовую премию и вызываем calculate_bonus
        if worker_role_id == 6:
            base = (
                turnover_data[2] +
                mobile_bank[0] +
                activated_cards[0] +
                prems_dates[1]
            )
        elif worker_role_id == 8:
            base = (
                turnover_data[3] +
                mobile_bank[0] +
                activated_cards[0] +
                prems_dates[2]
            )
        else:
            logger.error(f"Unsupported role_id={worker_role_id}, owner={owner_name}")
            raise UndefinedRoleError("Role ID must be 6 or 8")

        bonus = calculate_bonus(base, service_qualities[2], service_qualities[1], worker_data[2])

        return Accountant(
            name=owner_name,
            salary=worker_data[2],
            bonus=bonus
        )

    def _get_turnover_out_balance_debt_osd(self, owner_name: str):
        self.cursor.execute(
            count_user_cards_turnover_out_balance_debt_osd,
            {"owner_name": owner_name, "year": self.year, "month": self.month}
        )
        res = self.cursor.fetchone()
        return res if res is not None else (0.0, 0.0, 0.0)

    def _get_service_qualities_balls(self, owner_id: int):
        self.cursor.execute(
            count_service_qualities_balls,
            {"owner_id": owner_id, "year": self.year, "month": self.month}
        )
        res = self.cursor.fetchone()
        return res if res is not None else (0.0, 0.0, 0.0)

    def _get_mobile_bank_perms(self, owner_id: int):
        self.cursor.execute(
            count_mobile_bank_perms,
            {"owner_id": owner_id, "year": self.year, "month": self.month}
        )
        res = self.cursor.fetchone()
        return res if res is not None else (0.0,)

    def _get_overdraft_perm(self, owner_id: int):
        self.cursor.execute(
            sql.SQL(count_overdraft_perm),
            {"owner_id": owner_id, "year": self.year, "month": self.month}
        )
        res = self.cursor.fetchone()
        return res if res is not None else (0.0,)

    def _get_worker_data(self, owner_id: int):
        self.cursor.execute(get_worker_data, {"owner_id": owner_id})
        return self.cursor.fetchone()

    def _get_activated_card_perms(self, owner_id: int):
        self.cursor.execute(count_activated_card_sales, {"owner_id": owner_id})
        res = self.cursor.fetchone()
        return res if res is not None else (0.0,)

    def _get_position_from_role_id(self, owner_id: int) -> Union[int, None]:
        self.cursor.execute(get_role_id_by_worker_id, {"owner_id": owner_id})
        res = self.cursor.fetchone()
        return res[0] if (res and res[0] is not None) else None

    def _create_accountant_excel(self, accountants: List[Accountant]) -> str:
        wb = openpyxl.load_workbook(
            self.automation_configs.def_excel_paths.def_report_template_accountant
        )
        ws = wb.active

        mapping = self._data_mapping_accountant(accountants)
        for cell, val in mapping.items():
            ref = get_writable_cell_ref(ws, cell)
            ws[ref].value = val

        timestamp = int(time.time())
        name = sanitize_filename(f"accountant_{timestamp}")
        date_dir = os.path.join(
            self.automation_configs.def_out_paths.accountant_dir_reports,
            get_current_date(self.month, self.year)
        )
        os.makedirs(date_dir, exist_ok=True)

        out_path = os.path.join(date_dir, f"{name}.xlsx")
        wb.save(out_path)
        return out_path

    def _data_mapping_accountant(self, accountants: List[Accountant]) -> dict:
        mapping = {"A2": "Месяц: " + get_current_date(self.month, self.year)}
        row = 5
        for acct in accountants:
            mapping[f"A{row}"] = acct.name
            mapping[f"B{row}"] = acct.salary
            mapping[f"C{row}"] = round(acct.bonus, 2)
            row += 1
        return mapping

    def _create_excel_accountants(self, data: List[Accountant]):
        self.file_path = self._create_accountant_excel(data)
        logger.info(f"Generated Excel: {self.file_path}")

    def create_reports_xlsx(self, month: int, year: int) -> str:
        self.month = month
        self.year = year
        accountants = self._get_accountant_data()
        if not accountants:
            raise NotFoundError("No accountant data found for given period")
        self._create_excel_accountants(accountants)
        return self.file_path
