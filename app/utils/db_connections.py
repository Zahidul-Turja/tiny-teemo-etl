import logging
import sqlite3
import traceback


class SQLiteDB:

    def __init__(self, file_name, table_name, columns, unique_together):
        self.file_name = file_name
        self.columns = columns
        self.table_name = table_name
        self.unique_together = unique_together

    def check_connection(self):
        try:
            connection = sqlite3.connect(f"../uploaded_files/{self.file_name}.db")
            connection.cursor()

            return True
        except sqlite3.Error as e:
            traceback.print_exc()
            return False
        finally:
            if connection:
                connection.close()
                print("Connection closed")

    def create_table(self):
        try:
            with sqlite3.connect(
                f"../uploaded_files/{self.file_name}.db"
            ) as connection:
                cursor = connection.cursor()

                columns_query = ""
                for idx, col in enumerate(self.columns):
                    query = f"{col.name} {col.type}"

                    if col.length:
                        # for CHAR and VARCHAR
                        query += f"({col.length})"

                    if col.is_primary_key:
                        query += " PRIMARY KEY"
                    elif col.is_unique:
                        query += " UNIQUE"

                    if col.auto_increment:
                        query += " AUTOINCREMENT"

                    if col.is_null:
                        query += " NULL"
                    else:
                        query += "NOT NULL"

                    columns_query = columns_query + ", " + query

                create_query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} ({columns_query})
                """

                cursor.execute(create_query)

                connection.commit()
        except Exception as e:
            traceback.print_exc()
            print(e)

        finally:
            if connection:
                connection.close()
                print("SQLite connection closed")

    def read_table(self, column_names):
        try:
            with sqlite3.connect(
                f"../uploaded_files/{self.file_name}.db"
            ) as connection:
                cursor = connection.cursor()

                if not column_names:
                    query = f"SELECT * FROM {self.table_name}"
                else:
                    columns = ",".join(column_names)
                    query = f"SELECT {columns} FROM {self.table_name}"

                cursor.execute(query)

        except Exception as e:
            traceback.print_exc()


# """
# columns [
#     {
#         "name": id,
#         "type": int,
#         "is_primary_key": True,
#         "is_unique": True,
#         "auto_increment": True,
#         "is_null": False,
#         "length": 100,
#     }
# ]
# """

# "unique_together": [
#             "column_1",
#             "column_2",
#         ]
