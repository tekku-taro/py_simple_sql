import unittest
from unittest.mock import Mock, patch
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dbquery.transaction_manager import TransactionManager


class TestTransactionManager(unittest.TestCase):
    def setUp(self):
        self.mock_connection = Mock()
        self.transaction_manager = TransactionManager(self.mock_connection)

    def test_transaction_commit(self):
        # トランザクション内で例外が発生しないケース
        with self.transaction_manager.transaction():
            # トランザクション内の操作をシミュレート
            pass
        
        # begin_transactionが呼ばれたことを確認
        self.mock_connection.begin_transaction.assert_called_once()
        # commitが呼ばれたことを確認
        self.mock_connection.commit.assert_called_once()
        # rollbackは呼ばれていないことを確認
        self.mock_connection.rollback.assert_not_called()

    def test_transaction_rollback(self):
        # トランザクション内で例外が発生するケース
        try:
            with self.transaction_manager.transaction():
                # 例外を発生させる
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # begin_transactionが呼ばれたことを確認
        self.mock_connection.begin_transaction.assert_called_once()
        # commitは呼ばれていないことを確認
        self.mock_connection.commit.assert_not_called()
        # rollbackが呼ばれたことを確認
        self.mock_connection.rollback.assert_called_once()

    def test_nested_transaction(self):
        # ネストされたトランザクションのシミュレーション
        # 実際の実装では、Connectionクラスでトランザクションレベルを管理する
        
        # Connectionクラスのモック設定
        with patch.object(self.mock_connection, 'begin_transaction') as mock_begin, \
             patch.object(self.mock_connection, 'commit') as mock_commit:
            
            # 外部トランザクション
            with self.transaction_manager.transaction():
                # 内部トランザクション
                with self.transaction_manager.transaction():
                    pass

            # begin_transactionとcommitが各1回ずつ呼ばれることを確認
            # 実際のConnectionクラスでは、ネスト回数に応じて内部カウンタを管理する
            self.assertEqual(mock_begin.call_count, 2)
            self.assertEqual(mock_commit.call_count, 2)


if __name__ == '__main__':
    unittest.main()