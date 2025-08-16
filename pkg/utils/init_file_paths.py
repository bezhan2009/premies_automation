import os
from configs.load_configs import get_config
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def ensure_directories_exist():
    """
    Проверяет существование всех директорий из конфига и создает их при необходимости.
    """
    config = get_config()

    # Список всех путей из конфига, которые нужно проверить/создать
    paths_to_check = [
        config.automation_details.def_template_paths.def_report_template_cards.rsplit('/', 1)[0],
        config.automation_details.def_template_paths.def_report_template_credits.rsplit('/', 1)[0],
        config.automation_details.def_template_paths.def_report_template_accountant.rsplit('/', 1)[0],
        config.automation_details.def_out_paths.accountant_dir_reports,
        config.automation_details.def_out_paths.output_dir_reports,
        config.automation_details.def_out_paths.zip_reports_file_path,
        config.automation_details.def_out_paths.uploaded_files_path,
    ]

    paths_to_check = list(set(paths_to_check))

    for path in paths_to_check:
        if not path:
            continue

        try:
            os.makedirs(path, exist_ok=True)
            logger.info(f"Директория проверена/создана: {path}")
        except Exception as e:
            logger.error(f"Ошибка при создании директории {path}: {str(e)}")
