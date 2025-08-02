from internal.service.automation.application_automation import ApplicationAutomation
from pkg.errors.not_found_error import NotFoundError
from pkg.logger.logger import setup_logger
from typing import List

logger = setup_logger(__name__)


def create_xlsx_file_applications(application_ids: List[int]) -> str:
    application = ApplicationAutomation()

    try:
        xlsx_path = application.create_reports_xlsx(application_ids)
        return xlsx_path
    except NotFoundError as nt:
        raise nt
    except Exception as e:
        logger.error("Error while creating application reports: {}".format(e))
        raise e
