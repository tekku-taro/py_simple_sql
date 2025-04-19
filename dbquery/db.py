from typing import Any, Dict, List
from contextlib import contextmanager
from dbquery.transaction_manager import TransactionManager
from dbquery.query_builder import QueryBuilder
from dbquery.connection import Connection
from dbquery.sqlite_connection import SQLiteConnection
from dbquery.mysql_connection import MySQLConnection
from dbquery.postgresql_connection import PostgreSQLConnection


class DB:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = self._create_connection()
        self.connection.connect()
        self.transaction_manager = TransactionManager(self.connection)

    def _create_connection(self) -> Connection:
        """適切なコネクションオブジェクトを作成します"""
        driver = self.config.get("driver", "sqlite").lower()
        
        if driver == "sqlite":
            return SQLiteConnection(self.config)
        elif driver == "mysql":
            return MySQLConnection(self.config)
        elif driver == "postgresql":
            return PostgreSQLConnection(self.config)
        else:
            raise ValueError(f"Unsupported database driver: {driver}")

    def table(self, table_name: str) -> QueryBuilder:
        """新しいクエリビルダインスタンスを作成します"""
        return QueryBuilder(self.connection).table(table_name)

    def raw(self, query: str, bindings: List[Any] = None) -> List[Dict[str, Any]]:
        """生SQLを実行します"""
        return self.connection.fetch_all(query, bindings)

    def raw_execute(self, query: str, bindings: List[Any] = None) -> bool:
        """生SQLを実行します（更新系）"""
        return self.connection.execute(query, bindings)

    @contextmanager
    def transaction(self):
        """トランザクションを開始します"""
        with self.transaction_manager.transaction():
            yield

    def disconnect(self) -> None:
        """データベース接続を閉じます"""
        self.connection.disconnect()

    def __del__(self):
        """オブジェクト破棄時に接続を閉じます"""
        try:
            self.disconnect()
        except:
            pass

