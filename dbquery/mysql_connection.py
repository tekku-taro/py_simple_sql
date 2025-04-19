from typing import Any, Dict, List, Optional
from dbquery.connection import Connection
import mysql.connector


class MySQLConnection(Connection):
    def connect(self) -> None:
        self._connection = mysql.connector.connect(
            host=self.config.get("host", "localhost"),
            user=self.config.get("user", "root"),
            password=self.config.get("password", ""),
            database=self.config["database"],
            port=self.config.get("port", 3306)
        )

    def disconnect(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None

    def begin_transaction(self) -> None:
        self._transaction_level += 1
        if self._transaction_level == 1:
            self._connection.start_transaction()

    def commit(self) -> None:
        self._transaction_level = max(0, self._transaction_level - 1)
        if self._transaction_level == 0:
            self._connection.commit()

    def rollback(self) -> None:
        self._transaction_level = 0
        self._connection.rollback()

    def execute(self, query: str, bindings: List[Any] = None) -> bool:
        bindings = bindings or []
        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(query, bindings)
            if self._transaction_level == 0:
                self._connection.commit()
            return True
        except mysql.connector.Error as e:
            print(f"MySQL Database Error: {e}")
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
            # トランザクション管理外の場合のみコミットする
            if self._transaction_level == 0:
                self._connection.commit()
            return True
        except mysql.connector.Error as e:
            # エラー内容を標準出力に表示
            print(f"MySQL Database Error during executemany: {e}")
            # エラーを再発生させる
            raise e
        finally:
            # エラー発生有無に関わらず、cursorが開かれていれば閉じる
            if cursor:
                cursor.close()

    def fetch_all(self, query: str, bindings: List[Any] = None) -> List[Dict[str, Any]]:
        bindings = bindings or []
        cursor = self._connection.cursor(dictionary=True)
        cursor.execute(query, bindings)
        
        result = cursor.fetchall()
        cursor.close()
        
        return result

    def fetch_one(self, query: str, bindings: List[Any] = None) -> Optional[Dict[str, Any]]:
        bindings = bindings or []
        cursor = self._connection.cursor(dictionary=True)
        cursor.execute(query, bindings)
        
        result = cursor.fetchone()
        cursor.close()
        
        return result

    def last_insert_id(self) -> int:
        return self._connection.cursor().lastrowid

    def quote_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"

    def get_placeholder(self) -> str:
        return "%s"