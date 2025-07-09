from configs.load_configs import get_config
from internal.lib.date import get_current_date
from internal.lib.file import (
    move_and_rename_file,
    clear_folder
)
from internal.service.automation.reports_automation import ReportsAutomation


def create_zip_reports(month: int, year: int):
    reports_automation = ReportsAutomation()

    zip_file_name = reports_automation.create_reports_zip(month, year)
    zip_file_path = move_and_rename_file(str(
        reports_automation.configs.automation_details.def_out_paths.zip_reports_file_path + "\\" +
        get_current_date(month, year)), zip_file_name)

    clear_folder(get_config().automation_details.def_out_paths.output_dir_reports)

    return zip_file_path


def create_zip_report_one_employee(worker_id: int, month: int, year: int):
    reports_automation = ReportsAutomation()

    zip_file_name = reports_automation.create_report_zip_one_employee(worker_id, month, year)
    zip_file_path = move_and_rename_file(str(
        reports_automation.configs.automation_details.def_out_paths.zip_reports_file_path + "\\" +
        get_current_date(month, year)), zip_file_name)

    clear_folder(get_config().automation_details.def_out_paths.output_dir_reports)

    return zip_file_path
