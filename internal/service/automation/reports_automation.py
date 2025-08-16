import glob
import os
import time
from typing import List

import openpyxl
from psycopg2 import sql

from configs.load_configs import get_config
from internal.app.models.employee import Employee
from internal.lib.date import get_current_date
from internal.lib.encypter import hash_sha256
from internal.lib.file import get_writable_cell_ref, sanitize_filename
from internal.lib.zip import archive_directory
from internal.service.automation.base_automation import BaseAutomation
from internal.sql.reports import (
    count_user_cards_category_issued,
    count_user_cards_turnover_out_balance_debt_osd,
    count_service_qualities_balls,
    count_mobile_bank_perms,
    count_overdraft_perm,
    get_worker_data,
    get_worker_place_data,
    get_role_id_by_worker_id,
    count_activated_card_perms
)
from pkg.errors.not_found_error import NotFoundError
from pkg.errors.undefined_role_error import UndefinedRoleError
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


class ReportsAutomation(BaseAutomation):
    def __init__(self):
        self.automation_configs = get_config().automation_details
        super().__init__("service.automation.ReportsAutomation")

    def _get_employees_data(self) -> List[Employee]:
        employees_data = []
        for owner in self.owners:
            employee = self._collect_employee_data(owner)
            employees_data.append(employee)
        return employees_data

    def _get_employee_data(self, owner_id: int) -> Employee:
        for owner in self.owners:
            if owner[0] == owner_id:
                employee = self._collect_employee_data(owner)
                return employee

    def _collect_employee_data(self, owner) -> Employee:
        owner_id, owner_name = owner

        worker_data = self._get_worker_data(owner_id)

        worker_place_work_data = self._get_worker_place_work_data(worker_data[0])

        cards_category_issued = self._get_cards_category_issued(owner_name)

        turnover_data = self._get_turnover_out_balance_debt_osd(owner_name)

        service_qualities = self._get_service_qualities_balls(worker_data[0])

        mobile_bank = self._get_mobile_bank_perms(worker_data[0])

        overdraft = self._get_overdraft_perm(worker_data[0])

        activated_cards = self._get_activated_card_perms(worker_data[0])

        return Employee(
            name=owner_name,
            position=worker_data[1],
            salary=worker_data[2],
            salary_project=worker_data[3],
            plan=worker_data[4],
            place_work=worker_place_work_data[0],
            place_work_description=worker_place_work_data[1],
            type_worker=self._get_position_from_role_id(worker_data[0]),
            mobile_bank_sales=mobile_bank[0],
            overdraft_sales=overdraft[0],
            visa_mc_sales=cards_category_issued["MC"] + cards_category_issued["VISA"] + cards_category_issued[
                "MC Nonresident"] + cards_category_issued["VISA Nonresident"],
            korty_milly=cards_category_issued["Корти милли"],
            visa_mc_vip_sales=(
                cards_category_issued["VISA Signature"]
            ),
            visa_mc_business_sales=cards_category_issued["MC Business"] + cards_category_issued["Visa Business"],
            debt_osd_count=turnover_data[0],
            out_balance_count=turnover_data[1],
            cards_activated_sales=activated_cards[0],
            call_center_ball=service_qualities[2],
            complaint=service_qualities[0],
            test_ball=service_qualities[1]
        )

    def _get_cards_category_issued(self, owner_name: str) -> dict:
        categories = {
            "VISA": 0, "VISA Signature": 0, "Visa Business": 0, "VISA Nonresident": 0,
            "MC": 0, "MC Business": 0, "MC Nonresident": 0, "Корти милли": 0
        }
        self.cursor.execute(count_user_cards_category_issued, {"owner_name": hash_sha256(owner_name),
                                                               "year": self.year,
                                                               "month": self.month})

        for card_type, count in self.cursor.fetchall():
            categories[card_type.strip()] = count

        return categories

    def _get_turnover_out_balance_debt_osd(self, owner_name: str):
        self.cursor.execute(count_user_cards_turnover_out_balance_debt_osd, {"owner_name": hash_sha256(owner_name),
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

    def _get_worker_place_work_data(self, owner_id: int):
        self.cursor.execute(get_worker_place_data, {"owner_id": owner_id})
        res = self.cursor.fetchone()
        if res is None:
            return ["Not Found", "Not Found"]

        return res

    def _get_activated_card_perms(self, owner_id: int):
        self.cursor.execute(count_activated_card_perms, {"owner_id": owner_id})
        res = self.cursor.fetchone()
        if res is None:
            return [0.0]
        return res

    def _get_position_from_role_id(self, owner_id: int) -> str:
        self.cursor.execute(get_role_id_by_worker_id, {"owner_id": owner_id})
        res = self.cursor.fetchone()
        if res is None:
            raise NotFoundError("User/worker not found")

        role_id = res[0]

        match role_id:
            case 6:
                return "Корт"
            case 8:
                return "Карзхо ва дигар"
            case _:
                logger.error(f"[{self.OP}._get_position_from_role_id] Undefined role error")
                raise UndefinedRoleError("Undefined role")

    def _get_role_id_from_type_worker(self, type_worker: str) -> int:
        match type_worker:
            case "Корт":
                return 6
            case "Карзхо ва дигар":
                return 8
            case _:
                logger.error(f"[{self.OP}._get_role_id_from_type_worker] Undefined role error")
                raise UndefinedRoleError("Undefined role")

    def get_right_template_for_worker(self, type_worker: str) -> str:
        match type_worker:
            case "Корт":
                return self.automation_configs.def_template_paths.def_report_template_cards
            case "Карзхо ва дигар":
                return self.automation_configs.def_template_paths.def_report_template_credits
            case _:
                logger.error(f"[{self.OP}.get_right_template_for_worker] Undefined role error")
                raise UndefinedRoleError("Undefined role")

    def _create_employee_excel(self, employee: Employee):
        """Create an Excel file for the given employee using the template."""
        # Load the template workbook
        try:
            wb = openpyxl.load_workbook(self.get_right_template_for_worker(employee.type_worker))
            ws = wb.active  # Assuming the data goes into the active sheet
        except FileNotFoundError as ef:
            raise ef

        # Define where each piece of data should go in the template
        data_mapping = self._data_mapping_employee(employee)

        # Fill in the data, handling merged cells
        for cell_ref, value in data_mapping.items():
            writable_ref = get_writable_cell_ref(ws, cell_ref)
            ws[writable_ref].value = value

        # Save the new file
        timestamp = int(time.time())  # UNIX-время в секундах
        sanitized_name = sanitize_filename(f"{employee.name}_{timestamp}")
        output_filename = os.path.join(self.automation_configs.def_out_paths.output_dir_reports,
                                       f"{sanitized_name}.xlsx")
        wb.save(output_filename)
        return output_filename

    def _data_mapping_employee(self, employee: Employee) -> dict:
        role_id = self._get_role_id_from_type_worker(employee.type_worker)
        data_mapping = {}

        if role_id == 6:
            data_mapping = {
                "A7": employee.place_work,

                "C9": get_current_date(self.month, self.year),
                "C12": employee.name,
                "C14": employee.type_worker,
                "C16": employee.salary,

                "D22": employee.plan,
                "D26": employee.salary_project,
                "D27": employee.mobile_bank_sales,
                "D28": employee.overdraft_sales,

                "D30": employee.visa_mc_sales,
                "D31": employee.korty_milly,
                "D32": employee.visa_mc_vip_sales,
                "D33": employee.visa_mc_business_sales,

                "D36": employee.debt_osd_count,
                "D37": employee.out_balance_count,
                "D38": employee.cards_activated_sales,

                "D43": employee.call_center_ball,
                "D44": employee.complaint,
                "D45": employee.test_ball,
            }
        elif role_id == 8:
            data_mapping = {
                "A7": employee.place_work,

                "C9": get_current_date(self.month, self.year),
                "C12": employee.name,
                "C14": employee.type_worker,
                "C16": employee.position,
                "C18": employee.salary,

                "D28": employee.salary_project,
                "D29": employee.mobile_bank_sales,
                "D30": employee.overdraft_sales,

                "D32": employee.visa_mc_sales,
                "D33": employee.korty_milly,
                "D34": employee.visa_mc_vip_sales,
                "D35": employee.visa_mc_business_sales,

                "D38": employee.debt_osd_count,
                "D39": employee.out_balance_count,
                "D40": employee.cards_activated_sales,

                "D45": employee.call_center_ball,
                "D46": employee.complaint,
                "D47": employee.test_ball,
            }

        return data_mapping

    def _clear_output_directory(self):
        """Удаляет все xlsx и zip файлы из папки output_dir_reports перед новым запуском."""
        report_dir = self.automation_configs.def_out_paths.output_dir_reports
        for file_path in glob.glob(os.path.join(report_dir, "*")):
            os.remove(file_path)

    def _create_zip_excels_employees(self, employee_data: list[Employee]):
        """Process all employees and archive the results."""
        for employee in employee_data:
            output_file = self._create_employee_excel(employee)
            logger.info(f"Generated Excel file for {employee.name}: {output_file}")

        archive_directory(self.automation_configs.def_out_paths.output_dir_reports,
                          self.automation_configs.def_out_paths.zip_file_path)
        logger.info("Archived all employee Excel files into employee_excels.zip")

    def create_reports_zip(self, month: int, year: int) -> str:
        try:
            self.month = month
            self.year = year

            employee_data = self._get_employees_data()
            self._create_zip_excels_employees(employee_data)
        except NotFoundError as nt:
            raise nt
        except UndefinedRoleError as unr:
            raise unr
        except Exception as e:
            logger.error("Error while creating zip file: {}".format(str(e)))
            raise e
        except FileNotFoundError as ef:
            raise ef

        return self.automation_configs.def_out_paths.zip_file_path

    def create_report_zip_one_employee(self, owner_id: int, month: int, year: int) -> str:
        try:
            self.month = month
            self.year = year

            employee_data = self._get_employee_data(owner_id)
            employees_data = [employee_data]
            self._create_zip_excels_employees(employees_data)
        except NotFoundError as nt:
            raise nt
        except UndefinedRoleError as unr:
            raise unr
        except Exception as e:
            logger.error("Error while creating zip file: {}".format(str(e)))
            raise e
        except FileNotFoundError as ef:
            raise ef

        return self.automation_configs.def_out_paths.zip_file_path
