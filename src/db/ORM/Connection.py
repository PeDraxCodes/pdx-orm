import logging
import shutil
import sqlite3
import time
import traceback
from types import TracebackType

from db.ORM.BaseDBOperations import SqliteConnection

from . import settings
from .logger import ORM_LOGGER_NAME

orm_logger = logging.getLogger(ORM_LOGGER_NAME)


class Connection(sqlite3.Connection):

    def __init__(self, *args, **kwargs):
        super().__init__(settings.DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES, *args, **kwargs)

    def __enter__(self):
        self.execute("BEGIN TRANSACTION")
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None,
                 exc_tb: TracebackType | None):
        if exc_type:
            orm_logger.warning(f"Error: {exc_val}")
            tb_str = ''.join(traceback.format_tb(exc_tb))
            orm_logger.error(f"Traceback:\n{tb_str}")
            orm_logger.warning("Rolling back changes")
            self.rollback()
        else:
            self.commit()

        self.close()

    def _rollback_with_backup(self):
        orm_logger.warning(f"Rolling back with backup {self.BACKUP_DB}")
        backup_path = self.BACKUP_DB / self._backup_name
        try:
            self.close()
            if SqliteConnection.open:
                SqliteConnection.close()
            self.DB.unlink()
            try:
                backup_path.rename(backup_path)
                shutil.move(backup_path, self.DB)
            except OSError:
                time.sleep(0.2)
                shutil.move(backup_path, self.DB)
        except sqlite3.Error as e:
            logging.error(f"Error while rolling back: {e}")
            raise e
