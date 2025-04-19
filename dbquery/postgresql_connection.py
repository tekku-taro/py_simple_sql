from typing import Any, Dict, List, Optional
from dbquery.connection import Connection
import psycopg2


class PostgreSQLConnection(Connection):
    def connect(self) -> None:
        self._connection = psycopg2.connect(
            host=self.config.get("host", "localhost"),
            user=self.config.get("user", "postgres"),
            password=self.config.get("password", ""),
            dbname=self.config["database"],
            port=self.config.get("port", 5432)
        )
        self._connection.autocommit = True

    def disconnect(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None

    def begin_transaction(self) -> None:
        self._transaction_level += 1
        if self._transaction_level == 1:
            self._connection.autocommit = False

    def commit(self) -> None:
        self._transaction_level = max(0, self._transaction_level - 1)
        if self._transaction_level == 0:
            self._connection.commit()
            self._connection.autocommit = True

    def rollback(self) -> None:
        if self._transaction_level > 0: # トランザクションが開始されている場合のみロールバック
            try:
                self._connection.rollback()
            finally:
                # ロールバック後 (成功・失敗問わず) レベルをリセットし、autocommitを有効に戻す
                self._transaction_level = 0
                self._connection.autocommit = True

    def execute(self, query: str, bindings: List[Any] = None) -> bool:
        bindings = bindings or []
        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(query, bindings)
            return True
        except psycopg2.Error as e:
            print(f"PostgreSQL Database Error: {e}")
            raise e
        finally:
            if cursor:
                cursor.close()

    def execute_many(self, query: str, bindings_list: List[List[Any]] = None) -> bool:
        """
        同じクエリを複数のパラメータセットで実行します (バルクインサートなど)。
        成功した場合は True を返します。
        エラーが発生した場合は、エラーメッセージを出力し、例外を再発生させます。
        """
        bindings_list = bindings_list or []
        if not bindings_list:
            return True # 実行するデータがない場合は成功とする

        cursor = None
        try:
            cursor = self._connection.cursor()
            # executemany を使用してクエリを実行
            cursor.executemany(query, bindings_list)
            return True
        except psycopg2.Error as e:
            # エラー内容を標準出力に表示
            print(f"PostgreSQL Database Error during executemany: {e}")
            raise e
        finally:
            # エラー発生有無に関わらず、cursorが開かれていれば閉じる
            if cursor:
                cursor.close()


    def fetch_all(self, query: str, bindings: List[Any] = None) -> List[Dict[str, Any]]:
        bindings = bindings or []
        cursor = self._connection.cursor()
        cursor.execute(query, bindings)
        
        columns = [desc[0] for desc in cursor.description]
        result = []
        
        for row in cursor.fetchall():
            result.append(dict(zip(columns, row)))
            
        cursor.close()
        return result

    def fetch_one(self, query: str, bindings: List[Any] = None) -> Optional[Dict[str, Any]]:
        bindings = bindings or []
        cursor = self._connection.cursor()
        cursor.execute(query, bindings)
        
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        
        cursor.close()
        
        if row:
            return dict(zip(columns, row))
        
        return None

    def last_insert_id(self) -> int:
        cursor = self._connection.cursor()
        cursor.execute("SELECT lastval()")
        result = cursor.fetchone()[0]
        cursor.close()
        return result

    def quote_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'

    def get_placeholder(self) -> str:
        return "%s"
