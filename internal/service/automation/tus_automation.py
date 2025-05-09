from psycopg2 import sql

from internal.lib.date import (get_current_month, get_current_year)
from internal.lib.encypter import hash_sha256
from internal.repository.utils.utils import (get_users, insert_tus_marks)
from internal.sql.tus_marks import call_center_procent
from pkg.db.connect import (get_connection, get_cursor)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


class AutomationTusMarks:
    def __init__(self):
        self.OP = "service.automation.AutomationTusMarks"
        self.conn = get_connection()
        self.cursor = get_cursor()
        self.year = get_current_year()
        self.month = get_current_month()
        self.owners = self._fetch_owners()

    def _fetch_owners(self):
        try:
            return get_users(self.cursor)
        except Exception as e:
            logger.error(f"[{self.OP}] Error while selecting users: {e}")
            return []

    def set_average_score_owners(self):
        for owner in self.owners:
            try:
                values = {
                    "owner_name": hash_sha256(owner[1]),
                    "year": self.year,
                    "month": self.month,
                }

                self.cursor.execute(
                    sql.SQL(call_center_procent),
                    values
                )

                average_score = self.cursor.fetchone()
                if average_score is None:
                    continue

                insert_tus_marks(self.cursor, average_score[0], owner[0])
            except Exception as e:
                logger.error(f"[{self.OP}] Error while inserting workers average score: {e}")
                return False

        self.conn.commit()

        logger.info("[{self.OP}] Finished inserting workers average score.")

        return True
