"""
zer0data Client - Main interface for data access
"""

from typing import Optional
import clickhouse_connect


class Client:
    """Client for accessing zer0data from ClickHouse"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        database: str = "zer0data",
        username: str = "default",
        password: str = "",
    ):
        """
        Initialize ClickHouse client

        Args:
            host: ClickHouse host
            port: ClickHouse port
            database: Database name
            username: Username
            password: Password
        """
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
        )
        self.database = database

    def get_trades(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
    ):
        """
        Fetch historical trades

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            start_date: Start date (ISO format)
            end_date: End date (ISO format)

        Returns:
            Polars DataFrame with trade data
        """
        query = f"""
        SELECT *
        FROM {self.database}.trades
        WHERE symbol = '{symbol}'
          AND event_time >= parseDateTimeBestEffort('{start_date}')
          AND event_time < parseDateTimeBestEffort('{end_date}')
        ORDER BY event_time
        """
        result = self.client.query(query)
        return result.result_pl

    def get_liquidations(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
    ):
        """
        Fetch liquidations

        Args:
            symbol: Trading pair symbol
            start_date: Start date (ISO format)
            end_date: End date (ISO format)

        Returns:
            Polars DataFrame with liquidation data
        """
        query = f"""
        SELECT *
        FROM {self.database}.liquidations
        WHERE symbol = '{symbol}'
          AND event_time >= parseDateTimeBestEffort('{start_date}')
          AND event_time < parseDateTimeBestEffort('{end_date}')
        ORDER BY event_time
        """
        result = self.client.query(query)
        return result.result_pl

    def get_funding_rates(
        self,
        symbol: str,
        interval: str = "1h",
    ):
        """
        Get aggregated funding rates

        Args:
            symbol: Trading pair symbol
            interval: Time aggregation ('1h', '4h', '1d')

        Returns:
            Polars DataFrame with funding rate data
        """
        query = f"""
        SELECT *
        FROM {self.database}.funding_rates_agg
        WHERE symbol = '{symbol}' AND interval = '{interval}'
        ORDER BY window_start
        """
        result = self.client.query(query)
        return result.result_pl

    def query(self, sql: str):
        """
        Execute custom SQL query

        Args:
            sql: SQL query string

        Returns:
            Query result
        """
        return self.client.query(sql)

    def close(self):
        """Close the client connection"""
        self.client.close()
