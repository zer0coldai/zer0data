"""
zer0data - Binance perpetual futures data SDK
"""

__version__ = "0.1.0"

from zer0data.client import Client, ClientConfig
from zer0data.kline import KlineService
from zer0data.symbols import SymbolService

__all__ = ["__version__", "Client", "ClientConfig", "KlineService", "SymbolService"]
