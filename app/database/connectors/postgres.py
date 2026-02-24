from typing import Any, Dict

import psycopg2
from psycopg2.extras import RealDictCursor

from app.database.connectors.base import BaseDatabaseConnector


class PostgresConnector(BaseDatabaseConnector):

    def connect(self):
        try:
            db_credentials = self.connection
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

    def summarize(self, preview_rows: int = 5):
        try:
            self.connect()
            cursor = self._conn.cursor(cursor_factory=RealDictCursor)

            cursor.execute(
                """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema NOT in ('pg_catalog',  'information_schema')
                AND table_type = 'BASE TABLE';
            """
            )

            list_of_tables = [table["table_name"] for table in cursor.fetchall()]

            previews = []
            for table_name in list_of_tables:
                # Head of the table
                cursor.execute(f'SELECT * FROM "{table_name}" LIMIT {preview_rows}')
                table_data = [row for row in cursor.fetchall()]

                # Table META data
                query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = 'public'
                    AND table_name = %s
                ORDER BY ordinal_position;
                """
                cursor.execute(query, (table_name,))
                rows = cursor.fetchall()

                columns = [
                    {
                        "name": row["column_name"],
                        "type": row["data_type"],
                        "not_null": row["is_nullable"] == "NO",
                        "default": row["column_default"],
                        "primary_key": False,
                    }
                    for row in rows
                ]

                # Postgres stores primary keys separately
                pk_query = """
                SELECT
                    kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = 'public'
                    AND tc.table_name = %s;
                """
                cursor.execute(pk_query, (table_name,))

                pk_columns = {row["column_name"] for row in cursor.fetchall()}
                for col in columns:
                    col["primary_key"] = col["name"] in pk_columns

                # Number of rows
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                num_of_rows = cursor.fetchall()[0]["count"]

                data = {
                    "table": table_name,
                    "row_count": num_of_rows,
                    "columns": columns,
                    "data": table_data,
                }
                previews.append(data)

            return {
                "database": self.connection.database,
                "list_of_tables": list_of_tables,
                "previews": previews,
            }

        except Exception as e:
            raise Exception(f"Error summarizing: {str(e)}")
        finally:
            self.disconnect()

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
