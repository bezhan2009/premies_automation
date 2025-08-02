import os
import time
from datetime import datetime, date
from typing import List

import openpyxl

from configs.load_configs import get_config
from internal.app.models.application import ApplicationInfo
from internal.lib.date import get_current_date
from internal.lib.file import get_writable_cell_ref, sanitize_filename
from internal.repository.application import get_application_by_ids
from internal.service.automation.base_automation import BaseAutomation
from pkg.errors.not_found_error import NotFoundError
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


class ApplicationAutomation(BaseAutomation):
    def __init__(self):
        self.automation_configs = get_config().automation_details
        self.file_path = ""
        super().__init__("service.automation.ApplicationAutomation")

    def _get_application_data(self, application_ids: List[int]) -> List[ApplicationInfo]:
        return get_application_by_ids(application_ids)

    def _create_application_excel(self, applications: List[ApplicationInfo]) -> str:
        wb = openpyxl.load_workbook(
            self.automation_configs.def_excel_paths.def_report_template_applications
        )
        ws = wb.active

        mapping = self._data_mapping_application(applications)
        for cell, val in mapping.items():
            ref = get_writable_cell_ref(ws, cell)
            ws[ref].value = val

        timestamp = int(time.time())
        name = sanitize_filename(f"applications_{timestamp}")
        date_dir = os.path.join(
            self.automation_configs.def_out_paths.accountant_dir_reports,
            get_current_date(self.month, self.year)
        )
        os.makedirs(date_dir, exist_ok=True)

        out_path = os.path.join(date_dir, f"{name}.xlsx")
        wb.save(out_path)
        return out_path

    def _translate_resident(self, is_resident: bool) -> str:
        if is_resident:
            return "Да"
        else:
            return "Нет"

    def _format_date(self, value):
        if isinstance(value, datetime):
            return value.replace(tzinfo=None).strftime("%d.%m.%Y")
        elif isinstance(value, date):
            return value.strftime("%d.%m.%Y")
        return value

    def _data_mapping_application(self, applications: List[ApplicationInfo]) -> dict:
        mapping = {}
        row = 6
        for app in applications:
            mapping[f"A{row}"] = app.tax_workers_code
            mapping[f"B{row}"] = app.surname
            mapping[f"C{row}"] = app.name
            mapping[f"D{row}"] = app.patronymic
            mapping[f"E{row}"] = app.gender
            mapping[f"F{row}"] = self._format_date(app.birth_date)
            mapping[f"G{row}"] = self._translate_resident(app.is_resident)
            mapping[f"H{row}"] = app.type_of_certificate
            mapping[f"I{row}"] = app.documents_series
            mapping[f"J{row}"] = app.document_number
            mapping[f"K{row}"] = self._format_date(app.issued_at)
            mapping[f"L{row}"] = app.issued_by
            mapping[f"M{row}"] = app.country
            mapping[f"N{row}"] = app.region
            mapping[f"O{row}"] = app.district
            mapping[f"P{row}"] = app.population_type
            mapping[f"Q{row}"] = app.populated
            mapping[f"R{row}"] = app.street_type
            mapping[f"S{row}"] = app.street
            mapping[f"T{row}"] = app.house_number
            mapping[f"U{row}"] = app.corpus
            mapping[f"V{row}"] = app.apartment_number
            mapping[f"W{row}"] = app.phone_number
            mapping[f"X{row}"] = app.secret_word
            mapping[f"Y{row}"] = app.clients_code
            mapping[f"Z{row}"] = "972"
            mapping[f"AA{row}"] = app.embossed_name
            mapping[f"AB{row}"] = app.receiving_office
            row += 1
        return mapping

    def _create_excel_application(self, data: List[ApplicationInfo]):
        self.file_path = self._create_application_excel(data)
        logger.info(f"Generated Excel: {self.file_path}")

    def create_reports_xlsx(self, application_ids: List[int]) -> str:
        applications = self._get_application_data(application_ids)
        if not applications:
            raise NotFoundError("No application data is found for given period")
        self._create_excel_application(applications)
        return self.file_path
