import unittest
from unittest.mock import patch, MagicMock
import mysql.connector
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dbquery.mysql_connection import MySQLConnection


class TestMySQLConnection(unittest.TestCase):
    def setUp(self):
        self.config = {
            "database": "test_db",
            "host": "localhost",
            "user": "root",
            "password": "password",
            "port": 3306
        }
        
        # mysql.connector.connectをモックに置き換え
        self.mock_conn = MagicMock()
        self.patcher = patch('mysql.connector.connect', return_value=self.mock_conn)
        self.mock_connect = self.patcher.start()
        
        self.connection = MySQLConnection(self.config)

    def tearDown(self):
        self.patcher.stop()

    def test_connect(self):
        self.connection.connect()
        
        # mysql.connector.connectが正しいパラメータで呼ばれたことを確認
        self.mock_connect.assert_called_once_with(
            host="localhost",
            user="root",
            password="password",
            database="test_db",
            port=3306
        )

    def test_connect_with_defaults(self):
        # 最小限の設定
        minimal_config = {"database": "test_db"}
        connection = MySQLConnection(minimal_config)
        connection.connect()
        
        # デフォルト値が適用されていることを確認
        self.mock_connect.assert_called_once_with(
            host="localhost",
            user="root",
            password="",
            database="test_db",
            port=3306
        )

    def test_disconnect(self):
        self.connection._connection = self.mock_conn
        self.connection.disconnect()
        
        # closeが呼ばれたことを確認
        self.mock_conn.close.assert_called_once()
        # connectionがNoneにリセットされたことを確認
        self.assertIsNone(self.connection._connection)

    def test_begin_transaction(self):
        self.connection._connection = self.mock_conn
        
        # 最初のトランザクション開始
        self.connection.begin_transaction()
        self.assertEqual(self.connection._transaction_level, 1)
        self.mock_conn.start_transaction.assert_called_once()
        
        # 2回目のトランザクション開始（ネスト）
        self.mock_conn.reset_mock()
        self.connection.begin_transaction()
        self.assertEqual(self.connection._transaction_level, 2)
        # トランザクションレベルが1以上の場合はstart_transactionは実行されない
        self.mock_conn.start_transaction.assert_not_called()

    def test_commit(self):
        self.connection._connection = self.mock_conn
        self.connection._transaction_level = 2
        
        # 1回目のコミットはまだSQLコミットされない
        self.connection.commit()
        self.assertEqual(self.connection._transaction_level, 1)
        self.mock_conn.commit.assert_not_called()
        
        # 2回目のコミットでSQLコミットされる
        self.connection.commit()
        self.assertEqual(self.connection._transaction_level, 0)
        self.mock_conn.commit.assert_called_once()

    def test_rollback(self):
        self.connection._connection = self.mock_conn
        self.connection._transaction_level = 2
        
        # rollbackは即座にSQLをロールバックし、レベルを0にリセット
        self.connection.rollback()
        self.assertEqual(self.connection._transaction_level, 0)
        self.mock_conn.rollback.assert_called_once()

    def test_execute(self):
        self.connection._connection = self.mock_conn
        mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = mock_cursor
        
        # 成功ケース
        result = self.connection.execute("INSERT INTO users (name) VALUES (%s)", ["John"])
        
        # cursorが作成され、executeが呼ばれたことを確認
        self.mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with("INSERT INTO users (name) VALUES (%s)", ["John"])
        # コミットが呼ばれたことを確認
        self.mock_conn.commit.assert_called_once()
        # cursorがクローズされたことを確認
        mock_cursor.close.assert_called_once()
        self.assertTrue(result)
        
        # 失敗ケース
        self.mock_conn.reset_mock()
        mock_cursor.reset_mock()
        mock_cursor.execute.side_effect = mysql.connector.Error("Test error")
        # mysql.connector.Errorを発生させることを確認
        with self.assertRaises(mysql.connector.Error):
            self.connection.execute("INVALID SQL")
        # executeが呼び出されたことも確認
        mock_cursor.execute.assert_called_once_with("INVALID SQL", [])

    def test_fetch_all(self):
        self.connection._connection = self.mock_conn
        
        # モックデータ設定
        mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "John"},
            {"id": 2, "name": "Jane"}
        ]
        
        result = self.connection.fetch_all("SELECT * FROM users")
        
        # cursorが作成され、executeが呼ばれたことを確認
        self.mock_conn.cursor.assert_called_once_with(dictionary=True)
        mock_cursor.execute.assert_called_once_with("SELECT * FROM users", [])
        
        # 結果が正しいことを確認
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], {"id": 1, "name": "John"})
        self.assertEqual(result[1], {"id": 2, "name": "Jane"})
        
        # cursorがクローズされたことを確認
        mock_cursor.close.assert_called_once()

    def test_fetch_one(self):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = {"id": 1, "name": "Test"}
        self.connection._connection = mock_connection
        
        # Execute
        result = self.connection.fetch_one("SELECT * FROM test WHERE id = %s", [1])
        
        # Assert
        mock_connection.cursor.assert_called_once_with(dictionary=True)
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test WHERE id = %s", [1])
        mock_cursor.close.assert_called_once()
        self.assertEqual(result, {"id": 1, "name": "Test"})

    def test_fetch_one_with_no_bindings(self):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = {"id": 1, "name": "Test"}
        self.connection._connection = mock_connection
        
        # Execute
        result = self.connection.fetch_one("SELECT * FROM test")
        
        # Assert
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test", [])
        self.assertEqual(result, {"id": 1, "name": "Test"})

    def test_fetch_one_returns_none(self):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None
        self.connection._connection = mock_connection
        
        # Execute
        result = self.connection.fetch_one("SELECT * FROM test WHERE id = %s", [999])
        
        # Assert
        self.assertIsNone(result)

    def test_last_insert_id(self):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.lastrowid = 42
        self.connection._connection = mock_connection
        
        # Execute
        result = self.connection.last_insert_id()
        
        # Assert
        self.assertEqual(result, 42)

    def test_quote_identifier(self):
        # Execute
        result = self.connection.quote_identifier("table_name")
        
        # Assert
        self.assertEqual(result, "`table_name`")

if __name__ == "__main__":
    unittest.main()