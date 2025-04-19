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
        self.driver = self.config.get("driver", "sqlite").lower()
        self.connection = self._create_connection()
        self.connection.connect()
        self.transaction_manager = TransactionManager(self.connection)

    def _create_connection(self) -> Connection:
        """適切なコネクションオブジェクトを作成します"""
        
        if self.driver == "sqlite":
            return SQLiteConnection(self.config)
        elif self.driver == "mysql":
            return MySQLConnection(self.config)
        elif self.driver == "postgresql":
            return PostgreSQLConnection(self.config)
        else:
            raise ValueError(f"Unsupported database driver: {self.driver}")

    def table(self, table_name: str) -> QueryBuilder:
        """新しいクエリビルダインスタンスを作成します"""
        return QueryBuilder(self.connection).table(table_name)

    def _convert_placeholders(self, query: str) -> str:
        """SQLクエリ内のプレースホルダー「?」のみを「%s」に変換します
        
        SQLite以外のデータベースで使用するために、クエリ内の「?」プレースホルダーを「%s」に変換します。
        文字列リテラルや識別子内の「?」は変換しません。
        
        Args:
            query: 変換するSQL文字列
            
        Returns:
            変換後のSQL文字列
        """
        # SQLiteはすでに「?」を使用しているので変換不要
        if self.driver == "sqlite":
            return query
            
        result = ""
        in_string = False
        in_identifier = False  # バッククォートや二重引用符で囲まれた識別子
        string_char = None
        
        i = 0
        while i < len(query):
            char = query[i]
            
            # 文字列リテラルの開始/終了を検出
            if char in ["'", '"'] and (i == 0 or query[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif string_char == char:
                    in_string = False
            
            # バッククォートによる識別子の開始/終了を検出
            elif char == '`' and (i == 0 or query[i-1] != '\\'):
                in_identifier = not in_identifier
            
            # プレースホルダーの変換（文字列やバッククォート内でなければ）
            elif char == '?' and not in_string and not in_identifier:
                result += "%s"
                i += 1
                continue
                
            result += char
            i += 1
            
        return result

    def raw(self, query: str, bindings: List[Any] = None) -> List[Dict[str, Any]]:
        """生SQLを実行します
        
        どのデータベースでも「?」をプレースホルダーとして使用できます。
        内部で適切な変換を行います。
        
        Args:
            query: 実行するSQL文字列
            bindings: クエリパラメータのリスト
            
        Returns:
            クエリ結果の辞書のリスト
        """
        converted_query = self._convert_placeholders(query)
        return self.connection.fetch_all(converted_query, bindings)
    
    

    def raw_execute(self, query: str, bindings: List[Any] = None) -> bool:
        """生SQLを実行します（更新系）
        
        どのデータベースでも「?」をプレースホルダーとして使用できます。
        内部で適切な変換を行います。
        
        Args:
            query: 実行するSQL文字列
            bindings: クエリパラメータのリスト
            
        Returns:
            クエリが成功したかどうか
        """
        converted_query = self._convert_placeholders(query)
        return self.connection.execute(converted_query, bindings)

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

