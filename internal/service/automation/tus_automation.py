from contextlib import closing
from psycopg2 import sql
from typing import Dict, Optional, Tuple

from internal.lib.encypter import hash_sha256
from internal.repository.utils.utils import upsert_tus_marks
from internal.service.automation.base_automation import BaseAutomation
from internal.sql.general import get_worker_id_by_owner_name
from internal.sql.tus_marks import (
    call_center_procent,
    call_center_tests_and_complaints
)
from pkg.logger.logger import setup_logger

logger = setup_logger(__name__)


class AutomationTusMarks(BaseAutomation):
    def __init__(self):
        super().__init__("service.automation.AutomationTusMarks")

    def _get_worker_data(self, cursor, owner_name: str, month: int, year: int) -> Optional[Tuple[float, int, int, int]]:
        """Retrieve worker data including average score, tests, and complaints."""
        try:
            values = {
                "owner_name": hash_sha256(owner_name),
                "year": year,
                "month": month,
            }

            # Get average score
            cursor.execute(sql.SQL(call_center_procent), values)
            average_score = cursor.fetchone()
            if average_score is None:
                return None

            # Get tests and complaints
            cursor.execute(sql.SQL(call_center_tests_and_complaints), values)
            tests_and_complaints = cursor.fetchone() or (0, 0)

            # Get worker ID
            cursor.execute(
                sql.SQL(get_worker_id_by_owner_name),
                {"owner_name": owner_name}
            )
            worker_id = cursor.fetchone()

            if not worker_id:
                logger.warning(f"[{self.OP}] Worker not found: {owner_name}")
                return None

            return (average_score[0], tests_and_complaints[0], tests_and_complaints[1], worker_id[0])

        except Exception as e:
            logger.error(f"[{self.OP}] Error getting data for {owner_name}: {str(e)}")
            raise

    def set_average_score_owners(self, month: int, year: int) -> bool:
        """Calculate and set average TUS scores for all owners."""
        OP = f"{self.OP}.set_average_score_owners"
        success = True

        try:
            with closing(self.conn.cursor()) as cursor:
                for owner in self.owners:
                    try:
                        owner_name = owner[1]
                        worker_data = self._get_worker_data(cursor, owner_name, month, year)

                        if not worker_data:
                            continue

                        avg_score, tests, complaints, worker_id = worker_data
                        upsert_tus_marks(
                            cursor,
                            month,
                            year,
                            avg_score,
                            tests,
                            complaints,
                            worker_id
                        )

                    except Exception as e:
                        logger.error(f"[{OP}] Error processing {owner_name}: {str(e)}")
                        success = False
                        continue

            self.conn.commit()
            logger.info(f"[{OP}] Successfully processed TUS marks")
            return success

        except Exception as e:
            self.conn.rollback()
            logger.error(f"[{OP}] Transaction failed: {str(e)}")
            return False
