from typing import List

from internal.service.automation.poll_automation import PollAutomation
from pkg.errors.not_found_error import NotFoundError
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def create_poll(application_ids: List[int]) -> str:
    poll_automation = PollAutomation()

    try:
        poll_file_path = poll_automation.create_reports_docx(application_ids)
        return poll_file_path
    except NotFoundError as nt:
        raise nt
    except Exception as e:
        logger.error("Error while creating reports docx: {}".format(e))
        raise e


