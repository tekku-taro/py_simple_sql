import unittest
from unittest.mock import patch, MagicMock
import sqlite3
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dbquery.sqlite_connection import SQLiteConnection


class TestSQLiteConnection(unittest.TestCase):
    def setUp(self):
        self.config = {"database": ":memory:"}
        # sqlite3.connectをモックに置き換え
        self.mock_conn = MagicMock()
        self.patcher = patch('sqlite3.connect', return_value=self.mock_conn)
        self.mock_connect = self.patcher.start()
        
        # ConnectionオブジェクトとCursorオブジェクトのモック設定
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor
        
        self.connection = SQLiteConnection(self.config)

    def tearDown(self):
        self.patcher.stop()

    def test_connect(self):
        self.connection.connect()
        
        # sqlite3.connectが呼ばれたことを確認
        self.mock_connect.assert_called_once_with(":memory:")
        # row_factoryが設定されたことを確認
        self.assertEqual(self.connection._connection.row_factory, sqlite3.Row)

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
        self.mock_conn.execute.assert_called_once_with("BEGIN TRANSACTION")
        
        # 2回目のトランザクション開始（ネスト）
        self.mock_conn.reset_mock()
        self.connection.begin_transaction()
        self.assertEqual(self.connection._transaction_level, 2)
        # トランザクションレベルが1以上の場合はBEGIN TRANSACTIONは実行されない
        self.mock_conn.execute.assert_not_called()

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
        
        # 成功ケース
        self.mock_conn.execute.return_value = self.mock_cursor
        result = self.connection.execute("INSERT INTO users (name) VALUES (?)", ["John"])
        self.assertTrue(result)
        self.mock_conn.execute.assert_called_once_with("INSERT INTO users (name) VALUES (?)", ["John"])
        
        # 失敗ケース
        self.mock_conn.reset_mock()
        self.mock_conn.execute.side_effect = sqlite3.Error("Test error")
        # executeメソッドがsqlite3.Errorを発生させることを確認
        with self.assertRaises(sqlite3.Error):
            self.connection.execute("INVALID SQL")
        # executeが呼び出されたことも確認
        self.mock_conn.execute.assert_called_once_with("INVALID SQL", [])


    def test_fetch_all(self):
        self.connection._connection = self.mock_conn
        
        # モックデータ設定
        mock_row1 = MagicMock()
        mock_row1.keys.return_value = ["id", "name"]
        mock_row1.__getitem__.side_effect = lambda k: {"id": 1, "name": "John"}[k]
        
        mock_row2 = MagicMock()
        mock_row2.keys.return_value = ["id", "name"]
        mock_row2.__getitem__.side_effect = lambda k: {"id": 2, "name": "Jane"}[k]
        
        self.mock_cursor.fetchall.return_value = [mock_row1, mock_row2]
        
        result = self.connection.fetch_all("SELECT * FROM users")
        
        # cursorが作成され、executeが呼ばれたことを確認
        self.mock_conn.cursor.assert_called_once()
        self.mock_cursor.execute.assert_called_once_with("SELECT * FROM users", [])
        
        # 結果が正しく変換されていることを確認
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], {"id": 1, "name": "John"})
        self.assertEqual(result[1], {"id": 2, "name": "Jane"})

    def test_fetch_one(self):
        self.connection._connection = self.mock_conn
        
        # モックデータ設定
        mock_row = MagicMock()
        mock_row.keys.return_value = ["id", "name"]
        mock_row.__getitem__.side_effect = lambda k: {"id": 1, "name": "John"}[k]
        
        self.mock_cursor.fetchone.return_value = mock_row
        
        result = self.connection.fetch_one("SELECT * FROM users WHERE id = ?", [1])
        
        # cursorが作成され、executeが呼ばれたことを確認
        self.mock_conn.cursor.assert_called_once()
        self.mock_cursor.execute.assert_called_once_with("SELECT * FROM users WHERE id = ?", [1])
        
        # 結果が正しく変換されていることを確認
        self.assertEqual(result, {"id": 1, "name": "John"})
        
        # 結果がNoneの場合
        self.mock_cursor.reset_mock()
        self.mock_cursor.fetchone.return_value = None
        
        result = self.connection.fetch_one("SELECT * FROM users WHERE id = ?", [999])
        self.assertIsNone(result)

    def test_last_insert_id(self):
        self.connection._connection = self.mock_conn
        
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [42]  # 最後に挿入されたIDを42と仮定
        self.mock_conn.execute.return_value = mock_cursor
        
        result = self.connection.last_insert_id()
        
        # SQL文が実行されたことを確認
        self.mock_conn.execute.assert_called_once_with("SELECT last_insert_rowid()")
        self.assertEqual(result, 42)

    def test_quote_identifier(self):
        # SQLiteの識別子クォート
        result = self.connection.quote_identifier("table_name")
        self.assertEqual(result, '"table_name"')


if __name__ == '__main__':
    unittest.main()