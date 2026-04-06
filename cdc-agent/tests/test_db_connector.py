"""Tests for the DB connector abstraction."""

import sys
import os
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_simulator_mode_placeholder():
    with patch("db_connector.settings") as mock_settings:
        mock_settings.simulator_mode = True
        mock_settings.postgres_dsn = "host=localhost"
        from db_connector import DBConnector
        conn = DBConnector()
        assert conn.placeholder == "%s"
        assert conn.is_simulator is True


def test_hana_mode_placeholder():
    with patch("db_connector.settings") as mock_settings:
        mock_settings.simulator_mode = False
        from db_connector import DBConnector
        conn = DBConnector()
        assert conn.placeholder == "?"
        assert conn.is_simulator is False


def test_shadow_table_name_simulator():
    with patch("db_connector.settings") as mock_settings:
        mock_settings.simulator_mode = True
        from db_connector import DBConnector
        conn = DBConnector()
        name = conn.shadow_table_name("SAPSR3", "SALES_ORDERS")
        assert name == '"_EDELTA_CDC_SALES_ORDERS"'


def test_shadow_table_name_hana():
    with patch("db_connector.settings") as mock_settings:
        mock_settings.simulator_mode = False
        from db_connector import DBConnector
        conn = DBConnector()
        name = conn.shadow_table_name("SAPSR3", "SALES_ORDERS")
        assert name == '"SAPSR3"."_EDELTA_CDC_SALES_ORDERS"'


def test_purge_sql_postgres():
    with patch("db_connector.settings") as mock_settings:
        mock_settings.simulator_mode = True
        from db_connector import DBConnector
        conn = DBConnector()
        sql = conn.build_purge_sql("SAPSR3", "SALES_ORDERS")
        assert "INTERVAL" in sql
        assert "ADD_SECONDS" not in sql


def test_purge_sql_hana():
    with patch("db_connector.settings") as mock_settings:
        mock_settings.simulator_mode = False
        from db_connector import DBConnector
        conn = DBConnector()
        sql = conn.build_purge_sql("SAPSR3", "SALES_ORDERS")
        assert "ADD_SECONDS" in sql
        assert "INTERVAL" not in sql


def test_health_sql_simulator_returns_none():
    with patch("db_connector.settings") as mock_settings:
        mock_settings.simulator_mode = True
        from db_connector import DBConnector
        conn = DBConnector()
        assert conn.build_health_sql() is None


def test_health_sql_hana_returns_query():
    with patch("db_connector.settings") as mock_settings:
        mock_settings.simulator_mode = False
        from db_connector import DBConnector
        conn = DBConnector()
        sql = conn.build_health_sql()
        assert "M_HOST_RESOURCE_UTILIZATION" in sql
