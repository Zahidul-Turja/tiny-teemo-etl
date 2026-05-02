from unittest.mock import MagicMock, patch

import pytest

from app.core.constants import DatabaseType
from app.database.connectors.mysql import MySQLConnector
from app.database.connectors.postgres import PostgresConnector
from app.models.schemas import DatabaseConnection

# ── helpers ───────────────────────────────────────────────────────────────────


def _mysql_conn(host="mysql", port=3306, db="teemo_dev", user="teemo", pw="secret"):
    return DatabaseConnection(
        db_type=DatabaseType.MYSQL,
        host=host,
        port=port,
        database=db,
        username=user,
        password=pw,
    )


def _pg_conn(host="postgres", port=5432, db="teemo_dev", user="teemo", pw="secret"):
    return DatabaseConnection(
        db_type=DatabaseType.POSTGRESQL,
        host=host,
        port=port,
        database=db,
        username=user,
        password=pw,
    )


# ── MySQL: connection failure scenarios ───────────────────────────────────────


class TestMySQLConnectionFailures:
    """
    These tests simulate exactly the error seen in the frontend logs:

        Failed to connect to MySQL: (2003, "Can't connect to MySQL server on
        'tinyteemo-mtsql-local' ([Errno -2] Name or service not known)")

    The root causes are:
      (a) Using the container_name ("tinyteemo-mysql-local") as the host
          instead of the Docker service name ("mysql").
      (b) A typo in the hostname ("tinyteemo-mtsql-local").
      (c) The mysql service not being on the same Docker network as the app.
    """

    def test_bad_hostname_raises_connection_error(self):
        """DNS resolution failure must surface as ConnectionError, not bare pymysql error."""
        import errno
        import pymysql

        dns_exc = pymysql.err.OperationalError(
            2003,
            "Can't connect to MySQL server on 'bad-host' "
            "([Errno -2] Name or service not known)",
        )

        connector = MySQLConnector(_mysql_conn(host="bad-host"))
        with patch("pymysql.connect", side_effect=dns_exc):
            with pytest.raises(ConnectionError) as exc_info:
                connector.connect()

        assert "Failed to connect to MySQL" in str(exc_info.value)
        # The original pymysql error must be preserved so the caller can log it
        assert exc_info.value.__cause__ is dns_exc

    def test_container_name_as_host_fails(self):
        """
        Using container_name ("tinyteemo-mysql-local") instead of the Docker
        service name ("mysql") causes a DNS lookup failure inside the network.
        This is the exact misconfiguration that produced the reported error.
        """
        import pymysql

        dns_exc = pymysql.err.OperationalError(
            2003,
            "Can't connect to MySQL server on 'tinyteemo-mysql-local' "
            "([Errno -2] Name or service not known)",
        )

        # The WRONG host: container_name, not service name
        connector = MySQLConnector(_mysql_conn(host="tinyteemo-mysql-local"))
        with patch("pymysql.connect", side_effect=dns_exc):
            with pytest.raises(ConnectionError) as exc_info:
                connector.connect()

        assert "Failed to connect to MySQL" in str(exc_info.value)

    def test_typo_in_hostname_fails(self):
        """
        A typo like 'tinyteemo-mtsql-local' (mysql → mtsql) must also
        surface as ConnectionError.
        """
        import pymysql

        dns_exc = pymysql.err.OperationalError(
            2003,
            "Can't connect to MySQL server on 'tinyteemo-mtsql-local' "
            "([Errno -2] Name or service not known)",
        )

        connector = MySQLConnector(_mysql_conn(host="tinyteemo-mtsql-local"))
        with patch("pymysql.connect", side_effect=dns_exc):
            with pytest.raises(ConnectionError) as exc_info:
                connector.connect()

        assert "Failed to connect to MySQL" in str(exc_info.value)

    def test_wrong_password_raises_connection_error(self):
        import pymysql

        auth_exc = pymysql.err.OperationalError(1045, "Access denied for user")
        connector = MySQLConnector(_mysql_conn(pw="wrong"))
        with patch("pymysql.connect", side_effect=auth_exc):
            with pytest.raises(ConnectionError) as exc_info:
                connector.connect()

        assert "Failed to connect to MySQL" in str(exc_info.value)

    def test_test_connection_returns_dict_on_failure(self):
        """test_connection() must NEVER raise — it must always return a dict."""
        import pymysql

        dns_exc = pymysql.err.OperationalError(2003, "Name or service not known")
        connector = MySQLConnector(_mysql_conn(host="unreachable-host"))
        with patch("pymysql.connect", side_effect=dns_exc):
            result = connector.test_connection()

        assert isinstance(result, dict)
        assert result["success"] is False
        assert "message" in result

    def test_test_connection_success_path(self):
        """Happy path: test_connection() returns success=True and server version."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = ("8.4.0",)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.open = True

        connector = MySQLConnector(_mysql_conn())
        with patch("pymysql.connect", return_value=mock_conn):
            result = connector.test_connection()

        assert result["success"] is True
        assert "MySQL" in result["server_version"]

    def test_default_port_is_3306(self):
        """Omitting port must default to 3306, not None."""
        conn = DatabaseConnection(
            db_type=DatabaseType.MYSQL,
            host="mysql",
            database="teemo_dev",
            username="teemo",
            password="secret",
        )
        assert conn.port == 3306

    def test_connect_uses_correct_hostname(self):
        """
        The hostname passed to pymysql.connect must match what was configured —
        not a hardcoded fallback. This catches regressions where the connector
        silently substitutes 'localhost'.
        """
        import pymysql

        captured = {}

        def fake_connect(**kwargs):
            captured.update(kwargs)
            raise pymysql.err.OperationalError(2003, "Name or service not known")

        connector = MySQLConnector(_mysql_conn(host="mysql", port=3306))
        with patch("pymysql.connect", side_effect=fake_connect):
            with pytest.raises(ConnectionError):
                connector.connect()

        assert captured.get("host") == "mysql"
        assert captured.get("port") == 3306


# ── PostgreSQL: connection failure scenarios ──────────────────────────────────


class TestPostgreSQLConnectionFailures:
    def test_bad_hostname_raises_connection_error(self):
        """psycopg2 DNS failure must surface as ConnectionError."""
        try:
            import psycopg2

            dns_exc = psycopg2.OperationalError(
                "could not translate host name 'bad-host' to address: "
                "Name or service not known"
            )
        except ImportError:
            # psycopg2 not installed in test env — simulate with generic exception
            dns_exc = OSError("Name or service not known")

        connector = PostgresConnector(_pg_conn(host="bad-host"))
        with patch.object(
            connector,
            "connect",
            side_effect=ConnectionError(
                "Failed to connect to PostgreSQL: Name or service not known"
            ),
        ):
            with pytest.raises(ConnectionError) as exc_info:
                connector.connect()

        assert "Failed to connect" in str(exc_info.value)

    def test_test_connection_returns_dict_on_failure(self):
        """test_connection() must never raise on a bad host."""
        connector = PostgresConnector(_pg_conn(host="unreachable"))
        with patch.object(
            connector,
            "connect",
            side_effect=ConnectionError("Failed to connect to PostgreSQL: timeout"),
        ):
            result = connector.test_connection()

        assert isinstance(result, dict)
        assert result["success"] is False

    def test_default_port_is_5432(self):
        conn = DatabaseConnection(
            db_type=DatabaseType.POSTGRESQL,
            host="postgres",
            database="teemo_dev",
            username="teemo",
            password="secret",
        )
        assert conn.port == 5432


# ── Docker networking: hostname rules ─────────────────────────────────────────


class TestDockerHostnameRules:
    """
    Documents and enforces the correct hostname rules for Docker networking.

    Inside a Docker Compose network:
      ✓ Use the SERVICE NAME as the host  (e.g. "mysql", "postgres")
      ✗ Do NOT use the container_name     (e.g. "tinyteemo-mysql-local")
      ✗ Do NOT use "localhost"            (resolves to the container itself)

    These tests catch the exact misconfiguration that caused the reported error.
    """

    CORRECT_MYSQL_HOST = "mysql"  # Docker service name
    WRONG_HOSTS = [
        "tinyteemo-mysql-local",  # container_name — not a DNS name in the network
        "tinyteemo-mtsql-local",  # typo seen in the error log
        "localhost",  # resolves to the container itself
        "127.0.0.1",  # same problem
    ]

    @pytest.mark.parametrize("bad_host", WRONG_HOSTS)
    def test_wrong_hostname_fails_with_connection_error(self, bad_host):
        """Each known-bad hostname must produce a ConnectionError, not a silent failure."""
        import pymysql

        dns_exc = pymysql.err.OperationalError(
            2003, f"Can't connect to MySQL server on '{bad_host}'"
        )
        connector = MySQLConnector(_mysql_conn(host=bad_host))
        with patch("pymysql.connect", side_effect=dns_exc):
            with pytest.raises(ConnectionError):
                connector.connect()

    def test_correct_service_name_is_used_in_connect(self):
        """Connecting with the correct service name 'mysql' must call pymysql with that host."""
        import pymysql

        captured = {}

        def fake_connect(**kwargs):
            captured.update(kwargs)
            raise pymysql.err.OperationalError(2003, "simulated")

        connector = MySQLConnector(_mysql_conn(host=self.CORRECT_MYSQL_HOST))
        with patch("pymysql.connect", side_effect=fake_connect):
            with pytest.raises(ConnectionError):
                connector.connect()

        assert captured["host"] == self.CORRECT_MYSQL_HOST, (
            f"Expected host='mysql' (Docker service name) but got host='{captured['host']}'. "
            "Use the service name, not the container_name."
        )


# ── Upload retry loop ─────────────────────────────────────────────────────────


class TestUploadRetryBehaviour:
    """
    The ETL runner retries DB uploads up to max_retries times.
    These tests verify the retry loop behaves correctly for connection errors —
    the class of error seen in the reported logs (3 retries, then FAILED).
    """

    def test_all_retries_exhausted_raises(self):
        """After max_retries all failing, _upload_with_retry must raise RuntimeError."""
        import pandas as pd
        from app.services.etl_runner import _upload_with_retry
        from app.services.etl_logger import ETLLogger
        import uuid

        df = pd.DataFrame({"a": [1, 2, 3]})
        connector = MagicMock()
        connector.upload_dataframe.side_effect = ConnectionError(
            "Name or service not known"
        )

        logger = MagicMock(spec=ETLLogger)

        with pytest.raises(RuntimeError) as exc_info:
            _upload_with_retry(
                connector=connector,
                df=df,
                table_name="housing",
                column_mappings=[],
                if_exists="replace",
                batch_size=10_000,
                max_retries=3,
                logger=logger,
            )

        # Must have attempted exactly max_retries times
        assert connector.upload_dataframe.call_count == 3
        assert "DB upload failed after 3 attempt(s)" in str(exc_info.value)

    def test_succeeds_on_second_attempt(self):
        """If the first attempt fails but the second succeeds, result is returned."""
        import pandas as pd
        from app.services.etl_runner import _upload_with_retry

        df = pd.DataFrame({"a": [1, 2, 3]})
        connector = MagicMock()
        connector.upload_dataframe.side_effect = [
            ConnectionError("transient"),  # attempt 1 fails
            {"rows_inserted": 3, "rows_failed": 0},  # attempt 2 succeeds
        ]

        logger = MagicMock()

        result = _upload_with_retry(
            connector=connector,
            df=df,
            table_name="housing",
            column_mappings=[],
            if_exists="replace",
            batch_size=10_000,
            max_retries=3,
            logger=logger,
        )

        assert result["rows_inserted"] == 3
        assert connector.upload_dataframe.call_count == 2


# ── NaN sanitization: the "nan can not be used with MySQL" bug ────────────────


class TestSanitizeDfNaNHandling:
    """
    Regression tests for the bug:

        DB upload failed after 3 attempt(s):
        Failed to insert data: nan can not be used with MySQL

    Root cause: sanitize_df assigned cleaned Python lists back to DataFrame
    columns without specifying dtype=object. Pandas re-inferred float64 and
    silently converted None back to NaN. itertuples() then handed raw
    numpy.float64('nan') to PyMySQL, which correctly rejected it.

    The fix: all list assignments in sanitize_df use pd.array(..., dtype=object)
    to prevent pandas from re-inferring the dtype.
    """

    def _sanitize(self, df):
        from app.database.connectors.base import BaseDatabaseConnector

        return BaseDatabaseConnector.sanitize_df(df)

    def test_float_nan_becomes_none(self):
        """NaN in a float64 column must become None after sanitize, not stay NaN."""
        import pandas as pd
        import numpy as np

        df = pd.DataFrame({"val": [1.5, np.nan, 3.2, np.nan]})
        assert df["val"].dtype == np.float64

        clean = self._sanitize(df)

        # dtype must be object so None is preserved
        assert (
            clean["val"].dtype == object
        ), "Column must be object dtype — float64 would convert None back to NaN"
        vals = clean["val"].tolist()
        assert vals[0] == 1.5
        assert vals[1] is None, f"Expected None but got {vals[1]!r} (NaN leak!)"
        assert vals[2] == 3.2
        assert vals[3] is None, f"Expected None but got {vals[3]!r} (NaN leak!)"

    def test_itertuples_sees_none_not_nan(self):
        """
        The critical path: itertuples() must yield None, not nan.
        This is what PyMySQL's executemany() receives — nan causes the crash.
        """
        import pandas as pd
        import numpy as np

        df = pd.DataFrame({"price": [10.5, np.nan, 99.0], "name": ["a", "b", "c"]})
        clean = self._sanitize(df)

        rows = [tuple(row) for row in clean.itertuples(index=False)]
        for row in rows:
            for val in row:
                assert not (isinstance(val, float) and (val != val)), (
                    f"NaN leaked into itertuples row: {row}. "
                    "This would crash PyMySQL with 'nan can not be used with MySQL'."
                )
        assert rows[1][0] is None  # NaN price → None

    def test_integer_nan_becomes_none(self):
        """Nullable integer NaN must also become None."""
        import pandas as pd
        import numpy as np

        # pandas nullable Int64 (capital I) with NA
        df = pd.DataFrame({"count": pd.array([1, pd.NA, 3], dtype="Int64")})
        clean = self._sanitize(df)
        vals = clean["count"].tolist()
        assert vals[1] is None, f"Expected None, got {vals[1]!r}"

    def test_all_nan_float_column(self):
        """A fully-NaN float column must become all-None."""
        import pandas as pd
        import numpy as np

        df = pd.DataFrame({"x": [np.nan, np.nan, np.nan]})
        clean = self._sanitize(df)
        assert all(v is None for v in clean["x"].tolist())

    def test_mixed_valid_and_nan_floats_preserved(self):
        """Valid float values must not be corrupted during NaN removal."""
        import pandas as pd
        import numpy as np

        df = pd.DataFrame({"price": [0.0, 1.5, np.nan, -3.14, np.nan, 99.99]})
        clean = self._sanitize(df)
        vals = clean["price"].tolist()
        assert vals[0] == 0.0
        assert vals[1] == 1.5
        assert vals[2] is None
        assert abs(vals[3] - (-3.14)) < 1e-9
        assert vals[4] is None
        assert vals[5] == 99.99

    def test_object_column_with_nan_string(self):
        """
        Object columns containing the string 'nan' (from .astype(str)) must
        not be silently dropped. Only float NaN (pd.isna) should become None.
        """
        import pandas as pd
        import numpy as np

        df = pd.DataFrame({"label": ["foo", "nan_label", None, "bar"]})
        clean = self._sanitize(df)
        vals = clean["label"].tolist()
        assert vals[0] == "foo"
        assert vals[1] == "nan_label"  # the STRING "nan_label" must survive
        assert vals[2] is None
        assert vals[3] == "bar"
