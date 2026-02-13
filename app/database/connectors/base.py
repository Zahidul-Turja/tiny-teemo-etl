from abc import ABC, abstractmethod
from typing import Any, Dict

from app.models.schemas import DatabaseConnection


class BaseDatabaseConnector(ABC):
    """Abstract base class for database connectors"""

    def __init__(self, connection: DatabaseConnection):
        self.connection = connection
        self._conn = None

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass

    @abstractmethod
    def test_connection(self) -> Dict[str, Any]:
        pass
