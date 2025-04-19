import unittest
from unittest.mock import patch, MagicMock
import psycopg2
from dbquery.postgresql_connection import PostgreSQLConnection

class TestPostgreSQLConnection(unittest.TestCase):
    
    def setUp(self):
        self.config = {
            "host": "test_host",
            "user": "test_user",
            "password": "test_password",
            "database": "test_db",
            "port": 5433
        }
        self.connection = PostgreSQLConnection(self.config)
        self.connection._transaction_level = 0
        
    @patch('psycopg2.connect')
    def test_connect(self, mock_connect):
        # Setup mock
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Execute
        self.connection.connect()
        
        # Assert
        mock_connect.assert_called_once_with(
            host="test_host",
            user="test_user",
            password="test_password",
            dbname="test_db",
            port=5433
        )
        self.assertEqual(self.connection._connection, mock_connection)

    def test_connect_with_default_values(self):
        # Setup
        self.connection.config = {"database": "test_db"}
        
        with patch('psycopg2.connect') as mock_connect:
            mock_connection = MagicMock()
            mock_connect.return_value = mock_connection
            
            # Execute
            self.connection.connect()
            
            # Assert
            mock_connect.assert_called_once_with(
                host="localhost",
                user="postgres",
                password="",
                dbname="test_db",
                port=5432
            )

    def test_disconnect(self):
        # Setup
        mock_connection = MagicMock()
        self.connection._connection = mock_connection
        
        # Execute
        self.connection.disconnect()
        
        # Assert
        mock_connection.close.assert_called_once()
        self.assertIsNone(self.connection._connection)

    def test_begin_transaction(self):
        # Setup
        mock_connection = MagicMock()
        self.connection._connection = mock_connection
        
        # Execute
        self.connection.begin_transaction()
        
        # Assert
        self.assertEqual(self.connection._transaction_level, 1)
        self.assertFalse(mock_connection.autocommit)
        
        # Execute again (nested transaction)
        self.connection.begin_transaction()
        
        # Assert
        self.assertEqual(self.connection._transaction_level, 2)

    def test_commit(self):
        # Setup
        mock_connection = MagicMock()
        self.connection._connection = mock_connection
        self.connection._transaction_level = 2
        
        # Execute (first commit in nested transaction)
        self.connection.commit()
        
        # Assert
        self.assertEqual(self.connection._transaction_level, 1)
        mock_connection.commit.assert_not_called()
        
        # Execute (final commit)
        self.connection.commit()
        
        # Assert
        self.assertEqual(self.connection._transaction_level, 0)
        mock_connection.commit.assert_called_once()
        self.assertTrue(mock_connection.autocommit)

    def test_rollback(self):
        # Setup
        mock_connection = MagicMock()
        self.connection._connection = mock_connection
        self.connection._transaction_level = 3
        
        # Execute
        self.connection.rollback()
        
        # Assert
        self.assertEqual(self.connection._transaction_level, 0)
        mock_connection.rollback.assert_called_once()
        self.assertTrue(mock_connection.autocommit)

    def test_execute_success(self):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        self.connection._connection = mock_connection
        # connect() 後は autocommit=True, transaction_level=0 の状態をシミュレート
        mock_connection.autocommit = True
        self.connection._transaction_level = 0

        # Execute
        result = self.connection.execute("INSERT INTO test VALUES (%s, %s)", [1, "Test"])
        
        # Assert
        mock_connection.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with("INSERT INTO test VALUES (%s, %s)", [1, "Test"])
        mock_connection.commit.assert_not_called()
        mock_cursor.close.assert_called_once()
        self.assertTrue(result)


    def test_execute_failure(self):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = psycopg2.Error
        self.connection._connection = mock_connection
        
        # Execute
        # psycopg2.Errorを発生させることを確認
        with self.assertRaises(psycopg2.Error):
            result = self.connection.execute("INSERT INTO test VALUES (%s, %s)", [1, "Test"])
        # executeが呼び出されたことも確認
        mock_cursor.execute.assert_called_once_with("INSERT INTO test VALUES (%s, %s)", [1, "Test"])
        

    def test_fetch_all(self):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.description = [
            ("id", None, None, None, None, None, None),
            ("name", None, None, None, None, None, None)
        ]
        mock_cursor.fetchall.return_value = [(1, "Test1"), (2, "Test2")]
        self.connection._connection = mock_connection
        
        # Execute
        result = self.connection.fetch_all("SELECT * FROM test")
        
        # Assert
        mock_connection.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test", [])
        mock_cursor.close.assert_called_once()
        expected = [{"id": 1, "name": "Test1"}, {"id": 2, "name": "Test2"}]
        self.assertEqual(result, expected)

    def test_fetch_one(self):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.description = [
            ("id", None, None, None, None, None, None),
            ("name", None, None, None, None, None, None)
        ]
        mock_cursor.fetchone.return_value = (1, "Test")
        self.connection._connection = mock_connection
        
        # Execute
        result = self.connection.fetch_one("SELECT * FROM test WHERE id = %s", [1])
        
        # Assert
        mock_connection.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test WHERE id = %s", [1])
        mock_cursor.close.assert_called_once()
        self.assertEqual(result, {"id": 1, "name": "Test"})

    def test_fetch_one_returns_none(self):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.description = [
            ("id", None, None, None, None, None, None),
            ("name", None, None, None, None, None, None)
        ]
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
        mock_cursor.fetchone.return_value = (42,)
        self.connection._connection = mock_connection
        
        # Execute
        result = self.connection.last_insert_id()
        
        # Assert
        mock_connection.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT lastval()")
        mock_cursor.close.assert_called_once()
        self.assertEqual(result, 42)

    def test_quote_identifier(self):
        # Execute
        result = self.connection.quote_identifier("table_name")
        
        # Assert
        self.assertEqual(result, '"table_name"')

if __name__ == "__main__":
    unittest.main()