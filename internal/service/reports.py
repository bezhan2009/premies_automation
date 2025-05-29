from configs.load_configs import get_config
from internal.lib.date import get_current_date
from internal.lib.file import (
    move_and_rename_file,
    clear_folder
)
from internal.service.automation.reports_automation import ReportsAutomation


def create_zip_reports():
    reports_automation = ReportsAutomation()

    zip_file_name = reports_automation.create_reports_zip()
    zip_file_path = move_and_rename_file(str("output_excels\\" + get_current_date()), zip_file_name)

    clear_folder(get_config().automation_details.def_out_paths.output_dir_reports)

    return zip_file_path
