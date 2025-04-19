from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class Connection(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._connection = None
        self._transaction_level = 0

    @abstractmethod
    def connect(self) -> None:
        """データベースに接続します"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """データベース接続を閉じます"""
        pass

    @abstractmethod
    def begin_transaction(self) -> None:
        """トランザクションを開始します"""
        pass

    @abstractmethod
    def commit(self) -> None:
        """トランザクションをコミットします"""
        pass

    @abstractmethod
    def rollback(self) -> None:
        """トランザクションをロールバックします"""
        pass

    @abstractmethod
    def execute(self, query: str, bindings: List[Any] = None) -> bool:
        """単一のSQLクエリを実行します """
        pass

    @abstractmethod
    def execute_many(self, query: str, bindings_list: List[List[Any]] = None) -> bool:
        """同じSQLクエリを複数のパラメータセットで効率的に実行します """
        pass

    @abstractmethod
    def fetch_all(self, query: str, bindings: List[Any] = None) -> List[Dict[str, Any]]:
        """クエリを実行して全ての結果を取得します"""
        pass

    @abstractmethod
    def fetch_one(self, query: str, bindings: List[Any] = None) -> Optional[Dict[str, Any]]:
        """クエリを実行して最初の1件を取得します"""
        pass

    @abstractmethod
    def last_insert_id(self) -> int:
        """最後に挿入したレコードのIDを取得します"""
        pass

    @abstractmethod
    def quote_identifier(self, identifier: str) -> str:
        """識別子をクォートします"""
        pass

    @abstractmethod
    def get_placeholder(self) -> str:
        """プレースホルダの種類を返します"""
        pass