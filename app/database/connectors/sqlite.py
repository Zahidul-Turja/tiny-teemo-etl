import sqlite3
from typing import Any, Dict

from app.database.connectors.base import BaseDatabaseConnector


class SQLiteConnector(BaseDatabaseConnector):

    def connect(self):
        try:
            db_path = self.connection.database
            self._conn = sqlite3.connect(db_path)
            self._conn.execute(
                "PRAGMA foreign_keys = ON"
            )  # Enable the enforcement of foreign key constraints for the current database connection

        except sqlite3.Error as e:
            raise ConnectionError(f"Failed to connect to SQLite: {str(e)}")

    def disconnect(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def test_connection(self) -> Dict[str, Any]:
        try:
            self.connect()

            cursor = self._conn.cursor()
            cursor.execute("SELECT sqlite_version();")
            version = cursor.fetchone()[0]

            return {
                "success": True,
                "message": "Connection successful",
                "server_version": f"SQLite {version}",
                "database": self.connection.database,
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}",
            }
