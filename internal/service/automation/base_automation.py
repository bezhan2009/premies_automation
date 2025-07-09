from internal.lib.date import get_current_month, get_current_year
from internal.repository.utils.utils import get_workers
from pkg.db.connect import get_connection, get_cursor
from pkg.logger.logger import setup_logger
from configs.load_configs import get_config


logger = setup_logger(__name__)


class BaseAutomation:
    def __init__(self, op: str, use_month: bool = True, use_year: bool = True):
        self.OP = op
        self.configs = get_config()
        self.conn = get_connection()
        self.cursor = get_cursor()
        if use_month:
            self.month = get_current_month()
        if use_year:
            self.year = get_current_year()
        self.owners = self._fetch_owners()

    def _fetch_owners(self):
        try:
            return get_workers(self.cursor)
        except Exception as e:
            logger.error(f"[{self.OP}] Error while selecting users: {e}")
            return []
