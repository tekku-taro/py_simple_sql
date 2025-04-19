import unittest
from unittest.mock import Mock, patch
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dbquery.db import DB
from dbquery.sqlite_connection import SQLiteConnection
from dbquery.mysql_connection import MySQLConnection
from dbquery.postgresql_connection import PostgreSQLConnection


class TestDB(unittest.TestCase):
    def setUp(self):
        # モックコネクションを作成
        self.mock_sqlite_conn = Mock(spec=SQLiteConnection)
        self.mock_mysql_conn = Mock(spec=MySQLConnection)
        self.mock_postgresql_conn = Mock(spec=PostgreSQLConnection)
        
        # DB インスタンスをパッチしてモックコネクションを使用
        self.db_patcher = patch.object(DB, '_create_connection')
        # パッチを開始し、そのモックオブジェクトを保持する
        self.mock_create_connection = self.db_patcher.start()
        
        # 設定オブジェクト
        self.sqlite_config = {"driver": "sqlite", "database": ":memory:"}
        self.mysql_config = {"driver": "mysql", "host": "localhost", "user": "test", "password": "test", "database": "test"}
        self.postgresql_config = {"driver": "postgresql", "host": "localhost", "user": "test", "password": "test", "database": "test"}

    def tearDown(self):
        self.db_patcher.stop()

    def test_raw_sqlite(self):
        """SQLiteでのraw()メソッドのテスト"""
        # コネクションのモックを設定
        self.mock_create_connection.return_value = self.mock_sqlite_conn
        self.mock_sqlite_conn.__class__ = SQLiteConnection
        expected_result = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
        self.mock_sqlite_conn.fetch_all.return_value = expected_result
        
        # DBインスタンスを作成
        db = DB(self.sqlite_config)
        
        # テスト実行
        query = "SELECT * FROM users WHERE name = ?"
        bindings = ["John"]
        result = db.raw(query, bindings)
        
        # アサーション
        self.mock_sqlite_conn.fetch_all.assert_called_once_with(query, bindings)
        self.assertEqual(result, expected_result)

    def test_raw_mysql(self):
        """MySQLでのraw()メソッドのテスト（プレースホルダー変換を検証）"""
        # コネクションのモックを設定
        self.mock_create_connection.return_value = self.mock_mysql_conn
        self.mock_mysql_conn.__class__ = MySQLConnection
        expected_result = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
        self.mock_mysql_conn.fetch_all.return_value = expected_result
        
        # DBインスタンスを作成
        db = DB(self.mysql_config)
        
        # テスト実行
        query = "SELECT * FROM users WHERE name = ?"
        expected_query = "SELECT * FROM users WHERE name = %s"
        bindings = ["John"]
        result = db.raw(query, bindings)
        
        # アサーション
        self.mock_mysql_conn.fetch_all.assert_called_once_with(expected_query, bindings)
        self.assertEqual(result, expected_result)

    def test_raw_execute_postgresql(self):
        """PostgreSQLでのraw_execute()メソッドのテスト（プレースホルダー変換を検証）"""
        # コネクションのモックを設定
        self.mock_create_connection.return_value = self.mock_postgresql_conn
        self.mock_postgresql_conn.__class__ = PostgreSQLConnection
        self.mock_postgresql_conn.execute.return_value = True
        
        # DBインスタンスを作成
        db = DB(self.postgresql_config)
        
        # テスト実行
        query = "UPDATE users SET active = ? WHERE id = ?"
        expected_query = "UPDATE users SET active = %s WHERE id = %s"
        bindings = [True, 1]
        result = db.raw_execute(query, bindings)
        
        # アサーション
        self.mock_postgresql_conn.execute.assert_called_once_with(expected_query, bindings)
        self.assertTrue(result)

    def test_convert_placeholders_sqlite(self):
        """SQLiteではプレースホルダー変換を行わないことを確認"""
        # コネクションのモックを設定
        self.mock_create_connection.return_value = self.mock_sqlite_conn
        self.mock_sqlite_conn.__class__ = SQLiteConnection
        
        # DBインスタンスを作成
        db = DB(self.sqlite_config)
        
        # テスト対象のクエリ
        query = "SELECT * FROM users WHERE name = ? AND email LIKE '%?%' AND description = 'What?'"
        
        # 変換後のクエリが元のクエリと同じであることを確認
        result = db._convert_placeholders(query)
        self.assertEqual(result, query)

    def test_convert_placeholders_mysql_simple(self):
        """MySQL/PostgreSQLでの単純なプレースホルダー変換を確認"""
        # コネクションのモックを設定
        self.mock_create_connection.return_value = self.mock_mysql_conn
        self.mock_mysql_conn.__class__ = MySQLConnection
        
        # DBインスタンスを作成
        db = DB(self.mysql_config)
        
        # テスト対象のクエリとその期待値
        query = "SELECT * FROM users WHERE id = ? AND status = ?"
        expected = "SELECT * FROM users WHERE id = %s AND status = %s"
        
        # 変換後のクエリが期待通りであることを確認
        result = db._convert_placeholders(query)
        self.assertEqual(result, expected)

    def test_convert_placeholders_with_string_literals(self):
        """文字列リテラル内のクエスチョンマークは変換されないことを確認"""
        # コネクションのモックを設定
        self.mock_create_connection.return_value = self.mock_postgresql_conn
        self.mock_postgresql_conn.__class__ = PostgreSQLConnection
        
        # DBインスタンスを作成
        db = DB(self.postgresql_config)
        
        # テスト対象のクエリとその期待値
        query = "SELECT * FROM users WHERE name = ? AND bio LIKE 'What? Who?'"
        expected = "SELECT * FROM users WHERE name = %s AND bio LIKE 'What? Who?'"
        
        # 変換後のクエリが期待通りであることを確認
        result = db._convert_placeholders(query)
        self.assertEqual(result, expected)

    def test_convert_placeholders_with_mixed_quotes(self):
        """様々な引用符が混在する場合のプレースホルダー変換を確認"""
        # コネクションのモックを設定
        self.mock_create_connection.return_value = self.mock_mysql_conn
        self.mock_mysql_conn.__class__ = MySQLConnection
        
        # DBインスタンスを作成
        db = DB(self.mysql_config)
        
        # テスト対象のクエリとその期待値
        query = """SELECT * FROM users WHERE name = ? AND bio = "What's your name?" AND comment = 'Say "Hello?"' AND id = ?"""
        expected = """SELECT * FROM users WHERE name = %s AND bio = "What's your name?" AND comment = 'Say "Hello?"' AND id = %s"""
        
        # 変換後のクエリが期待通りであることを確認
        result = db._convert_placeholders(query)
        self.assertEqual(result, expected)

    def test_convert_placeholders_with_identifiers(self):
        """バッククォートで囲まれた識別子内のクエスチョンマークは変換されないことを確認"""
        # コネクションのモックを設定
        self.mock_create_connection.return_value = self.mock_mysql_conn
        self.mock_mysql_conn.__class__ = MySQLConnection
        
        # DBインスタンスを作成
        db = DB(self.mysql_config)
        
        # テスト対象のクエリとその期待値
        query = "SELECT * FROM `table?name` WHERE `column?` = ? AND id > ?"
        expected = "SELECT * FROM `table?name` WHERE `column?` = %s AND id > %s"
        
        # 変換後のクエリが期待通りであることを確認
        result = db._convert_placeholders(query)
        self.assertEqual(result, expected)

    def test_convert_placeholders_complex_query(self):
        """複雑なクエリでのプレースホルダー変換を確認"""
        # コネクションのモックを設定
        self.mock_create_connection.return_value = self.mock_postgresql_conn
        self.mock_postgresql_conn.__class__ = PostgreSQLConnection
        
        # DBインスタンスを作成
        db = DB(self.postgresql_config)
        
        # テスト対象のクエリとその期待値
        query = """
        SELECT u.*, 
               (SELECT COUNT(*) FROM posts WHERE user_id = u.id) as post_count 
        FROM users u 
        JOIN `user?details` ud ON ud.user_id = u.id 
        WHERE u.email LIKE '%?%' AND u.username = ? 
        AND u.description = 'What? Who?' 
        AND u.status IN (?, ?) 
        ORDER BY u.created_at DESC
        """
        expected = """
        SELECT u.*, 
               (SELECT COUNT(*) FROM posts WHERE user_id = u.id) as post_count 
        FROM users u 
        JOIN `user?details` ud ON ud.user_id = u.id 
        WHERE u.email LIKE '%?%' AND u.username = %s 
        AND u.description = 'What? Who?' 
        AND u.status IN (%s, %s) 
        ORDER BY u.created_at DESC
        """
        
        # 変換後のクエリが期待通りであることを確認
        result = db._convert_placeholders(query)
        self.assertEqual(result, expected)

    def test_convert_placeholders_escaped_quotes(self):
        """エスケープされた引用符を含むクエリでのプレースホルダー変換を確認"""
        # コネクションのモックを設定
        self.mock_create_connection.return_value = self.mock_mysql_conn
        self.mock_mysql_conn.__class__ = MySQLConnection
        
        # DBインスタンスを作成
        db = DB(self.mysql_config)
        
        # テスト対象のクエリとその期待値
        query = """SELECT * FROM users WHERE name = ? AND bio = 'He\\'s saying: "What?"' AND id = ?"""
        expected = """SELECT * FROM users WHERE name = %s AND bio = 'He\\'s saying: "What?"' AND id = %s"""
        
        # 変換後のクエリが期待通りであることを確認
        result = db._convert_placeholders(query)
        self.assertEqual(result, expected)

    def test_convert_placeholders_with_backticks_in_string_literals(self):
        """文字列リテラル内のバッククォートは識別子として扱われないことを確認"""
        # MySQLコネクションを使用する設定 (SQLite以外ならどれでも良い)
        self.mock_create_connection.return_value = self.mock_mysql_conn
        self.mock_mysql_conn.__class__ = MySQLConnection
        db = DB(self.mysql_config)

        # シングルクォート内のバッククォート
        query_single = "SELECT * FROM users WHERE name = ? AND comment = 'This is a `test` with a ? inside' AND status = ?"
        expected_single = "SELECT * FROM users WHERE name = %s AND comment = 'This is a `test` with a ? inside' AND status = %s"
        result_single = db._convert_placeholders(query_single)
        self.assertEqual(result_single, expected_single, "シングルクォート内のバッククォートが誤認識されている")

        # ダブルクォート内のバッククォート
        query_double = 'SELECT * FROM users WHERE name = ? AND comment = "Another `test` with a ? inside" AND status = ?'
        expected_double = 'SELECT * FROM users WHERE name = %s AND comment = "Another `test` with a ? inside" AND status = %s'
        result_double = db._convert_placeholders(query_double)
        self.assertEqual(result_double, expected_double, "ダブルクォート内のバッククォートが誤認識されている")

        # バッククォート識別子と文字列リテラル内のバッククォートが混在
        query_mixed = "SELECT `col?` FROM `table` WHERE name = ? AND comment = 'String with `backtick` and ?' AND id = ?"
        expected_mixed = "SELECT `col?` FROM `table` WHERE name = %s AND comment = 'String with `backtick` and ?' AND id = %s"
        result_mixed = db._convert_placeholders(query_mixed)
        self.assertEqual(result_mixed, expected_mixed, "識別子と文字列内のバッククォート混在時に誤認識されている")

        # エスケープされたバッククォートを含む文字列
        query_escaped = r"SELECT * FROM users WHERE name = ? AND data = 'This has an escaped \` backtick' AND value = ?"
        expected_escaped = r"SELECT * FROM users WHERE name = %s AND data = 'This has an escaped \` backtick' AND value = %s"
        result_escaped = db._convert_placeholders(query_escaped)
        self.assertEqual(result_escaped, expected_escaped, "エスケープされたバッククォートが誤認識されている")

if __name__ == '__main__':
    unittest.main()