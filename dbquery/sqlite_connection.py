from typing import Any, Dict, List, Optional
from dbquery.connection import Connection
import sqlite3


class SQLiteConnection(Connection):
    def connect(self) -> None:
        self._connection = sqlite3.connect(self.config["database"])
        self._connection.row_factory = sqlite3.Row

    def disconnect(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None

    def begin_transaction(self) -> None:
        self._transaction_level += 1
        if self._transaction_level == 1:
            self._connection.execute("BEGIN TRANSACTION")

    def commit(self) -> None:
        self._transaction_level = max(0, self._transaction_level - 1)
        if self._transaction_level == 0:
            self._connection.commit()

    def rollback(self) -> None:
        self._transaction_level = 0
        self._connection.rollback()

    def execute(self, query: str, bindings: List[Any] = None) -> bool:
        bindings = bindings or []
        try:
            self._connection.execute(query, bindings)
            return True
        except sqlite3.Error as e:
            # エラー内容を標準出力に表示
            print(f"Sqlite Database Error during execute: {e}")
            raise e

    def execute_many(self, query: str, bindings_list: List[List[Any]] = None) -> bool:
        bindings_list = bindings_list or [[]]
        try:
            self._connection.executemany(query, bindings_list)
            return True
        except sqlite3.Error as e:
            # エラー内容を標準出力に表示
            print(f"Sqlite Database Error during executemany: {e}")
            raise e


    def fetch_all(self, query: str, bindings: List[Any] = None) -> List[Dict[str, Any]]:
        bindings = bindings or []
        cursor = self._connection.cursor()
        cursor.execute(query, bindings)
        
        result = []
        for row in cursor.fetchall():
            result.append({key: row[key] for key in row.keys()})
        
        return result

    def fetch_one(self, query: str, bindings: List[Any] = None) -> Optional[Dict[str, Any]]:
        bindings = bindings or []
        cursor = self._connection.cursor()
        cursor.execute(query, bindings)
        
        row = cursor.fetchone()
        if row:
            return {key: row[key] for key in row.keys()}
        
        return None

    def last_insert_id(self) -> int:
        return self._connection.execute("SELECT last_insert_rowid()").fetchone()[0]

    def quote_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'

    def get_placeholder(self) -> str:
        return "?"