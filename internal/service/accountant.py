from internal.service.automation.accountant_automation import AccountantAutomation
from pkg.errors.not_found_error import NotFoundError
from pkg.errors.undefined_role_error import UndefinedRoleError
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


def create_report_for_accountant() -> str:
    logger.info('Creating report for accountant')
    try:
        accountant = AccountantAutomation()
        file_path = accountant.create_reports_xlsx()
    except NotFoundError as nt:
        raise nt
    except UndefinedRoleError as unr:
        raise unr
    except Exception as e:
        logger.error("Error while creating xlsx file: {}".format(str(e)))
        raise e

    logger.info('Created report for accountant: {}'.format(file_path))

    return file_path
