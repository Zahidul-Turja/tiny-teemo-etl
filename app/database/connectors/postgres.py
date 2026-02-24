from typing import Any, Dict

import psycopg2

from app.database.connectors.base import BaseDatabaseConnector


class PostgresConnector(BaseDatabaseConnector):

    def connect(self):
        try:
            db_credentials = self.connection
            print(db_credentials)
            self._conn = psycopg2.connect(
                host=db_credentials.host,
                database=db_credentials.database,
                user=db_credentials.username,
                password=db_credentials.password,
                port=db_credentials.port,
            )
        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to Postgres: {str(e)}")

    def disconnect(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def test_connection(self) -> Dict[str, Any]:
        try:
            self.connect()
            version = self._conn.server_version

            return {
                "success": True,
                "message": "Connection successful",
                "server_version": f"Postgres {version}",
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}",
            }
        finally:
            self.disconnect()

    def summarize(self):
        return super().summarize()

    def create_index(self, table_name, columns, index_name=None):
        return super().create_index(table_name, columns, index_name)

    def create_table(self, table_name, column_mappings):
        return super().create_table(table_name, column_mappings)

    def drop_table(self, table_name):
        return super().drop_table(table_name)

    def insert_data(self, table_name, df, batch_size=1000):
        return super().insert_data(table_name, df, batch_size)

    def table_exists(self, table_name):
        return super().table_exists(table_name)
