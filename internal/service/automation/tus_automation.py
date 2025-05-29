from psycopg2 import sql

from internal.lib.encypter import hash_sha256
from internal.repository.utils.utils import (upsert_tus_marks)
from internal.service.automation.base_automation import BaseAutomation
from internal.sql.general import get_worker_id_by_owner_name
from internal.sql.tus_marks import call_center_procent
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


class AutomationTusMarks(BaseAutomation):
    def __init__(self):
        super().__init__("service.automation.AutomationTusMarks")

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

                self.cursor.execute(
                    sql.SQL(get_worker_id_by_owner_name),
                    {
                        "owner_name": hash_sha256(owner[1]),
                    }
                )

                worker_id = self.cursor.fetchone()

                upsert_tus_marks(self.cursor, average_score[0], worker_id[0])
            except Exception as e:
                logger.error(f"[{self.OP}] Error while inserting workers average score: {e}")
                return False

        self.conn.commit()

        logger.info(f"[{self.OP}] Finished inserting workers average score.")

        return True
