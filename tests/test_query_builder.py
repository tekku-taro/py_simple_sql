import unittest
from unittest.mock import Mock, patch
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dbquery.query_builder import QueryBuilder


class TestQueryBuilder(unittest.TestCase):
    def setUp(self):
        self.mock_connection = Mock()
        self.mock_connection.get_placeholder.return_value = "?"
        self.builder = QueryBuilder(self.mock_connection)
        self.builder.table("users")

    def test_table(self):
        builder = QueryBuilder(self.mock_connection)
        result = builder.table("users")
        self.assertEqual(result._table, "users")
        self.assertEqual(result, builder)  # 自身を返すことを確認

    def test_select(self):
        # デフォルトは["*"]
        self.assertEqual(self.builder._columns, ["*"])
        
        # select指定
        result = self.builder.select("id", "name", "email")
        self.assertEqual(result._columns, ("id", "name", "email"))
        self.assertEqual(result, self.builder)  # 自身を返すことを確認

    def test_where(self):
        # 2引数形式 (=演算子省略)
        self.builder.where("name", "John")
        self.assertEqual(self.builder._where_clauses[0], ("name", "=", "John"))
        self.assertEqual(self.builder._bindings[0], "John")
        
        # 3引数形式
        self.builder.where("age", ">", 30)
        self.assertEqual(self.builder._where_clauses[1], ("age", ">", 30))
        self.assertEqual(self.builder._bindings[1], 30)

    def test_where_in(self):
        values = [1, 2, 3]
        self.builder.where_in("id", values)
        self.assertEqual(self.builder._where_clauses[0][0], "id IN (?, ?, ?)")
        self.assertEqual(self.builder._bindings, values)

    def test_where_not_in(self):
        values = [1, 2, 3]
        self.builder.where_not_in("id", values)
        self.assertEqual(self.builder._where_clauses[0][0], "id NOT IN (?, ?, ?)")
        self.assertEqual(self.builder._bindings, values)

    def test_order_by(self):
        self.builder.order_by("name", "asc")
        self.assertEqual(self.builder._order_by_clauses[0], ("name", "asc"))
        
        self.builder.order_by("age", "DESC")  # 大文字もOK
        self.assertEqual(self.builder._order_by_clauses[1], ("age", "desc"))  # 小文字に変換される

    def test_limit_offset(self):
        self.builder.limit(10).offset(5)
        self.assertEqual(self.builder._limit, 10)
        self.assertEqual(self.builder._offset, 5)

    def test_join(self):
        self.builder.join("contacts", "users.id", "=", "contacts.user_id")
        self.assertEqual(
            self.builder._join_clauses[0],
            ("inner", "contacts", "users.id", "=", "contacts.user_id")
        )
        
        self.builder.left_join("orders", "users.id", "=", "orders.user_id")
        self.assertEqual(
            self.builder._join_clauses[1],
            ("left", "orders", "users.id", "=", "orders.user_id")
        )
        
        self.builder.right_join("roles", "users.role_id", "=", "roles.id")
        self.assertEqual(
            self.builder._join_clauses[2],
            ("right", "roles", "users.role_id", "=", "roles.id")
        )

    def test_group_by(self):
        self.builder.group_by("department_id", "role_id")
        self.assertEqual(self.builder._group_by, ["department_id", "role_id"])

    def test_having(self):
        self.builder.having("count", ">", 10)
        self.assertEqual(self.builder._having_clauses[0], ("count", ">", 10))
        self.assertEqual(self.builder._bindings[0], 10)
        
        # 演算子省略形式
        self.builder.having("status", "active")
        self.assertEqual(self.builder._having_clauses[1], ("status", "=", "active"))
        self.assertEqual(self.builder._bindings[1], "active")

    def test_exists(self):
        # Mock返り値設定
        self.mock_connection.fetch_one.return_value = {"exists_flag": True}
        
        result = self.builder.exists()
        
        # exists queryがfetch_oneに渡されていることを確認
        self.mock_connection.fetch_one.assert_called_once()
        args = self.mock_connection.fetch_one.call_args[0]
        self.assertTrue("SELECT EXISTS(" in args[0])
        self.assertEqual(self.builder._columns, ["*"])  # 元の状態に戻されていることを確認
        self.assertTrue(result)  # True が返ることを確認

    def test_count(self):
        # Mock返り値設定
        self.mock_connection.fetch_one.return_value = {"aggregate": 5}
        
        result = self.builder.count()
        
        # COUNTクエリが実行されたことを確認
        self.mock_connection.fetch_one.assert_called_once()
        args = self.mock_connection.fetch_one.call_args[0]
        self.assertTrue("COUNT(*)" in args[0])
        self.assertEqual(result, 5)
        self.assertEqual(self.builder._columns, ["*"])  # 元の状態に戻されていることを確認

    def test_aggregate_functions(self):
        self.mock_connection.fetch_one.return_value = {"aggregate": 100}
        
        # MAX
        result = self.builder.max("price")
        self.mock_connection.fetch_one.assert_called()
        args = self.mock_connection.fetch_one.call_args[0]
        self.assertTrue("MAX(price)" in args[0])
        self.assertEqual(result, 100)
        
        # MIN
        self.mock_connection.fetch_one.reset_mock()
        self.mock_connection.fetch_one.return_value = {"aggregate": 10}
        result = self.builder.min("price")
        args = self.mock_connection.fetch_one.call_args[0]
        self.assertTrue("MIN(price)" in args[0])
        self.assertEqual(result, 10)
        
        # AVG
        self.mock_connection.fetch_one.reset_mock()
        self.mock_connection.fetch_one.return_value = {"aggregate": 55.5}
        result = self.builder.avg("price")
        args = self.mock_connection.fetch_one.call_args[0]
        self.assertTrue("AVG(price)" in args[0])
        self.assertEqual(result, 55.5)
        
        # SUM
        self.mock_connection.fetch_one.reset_mock()
        self.mock_connection.fetch_one.return_value = {"aggregate": 1000}
        result = self.builder.sum("price")
        args = self.mock_connection.fetch_one.call_args[0]
        self.assertTrue("SUM(price)" in args[0])
        self.assertEqual(result, 1000)

    def test_get(self):
        expected_result = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
        self.mock_connection.fetch_all.return_value = expected_result
        
        result = self.builder.get()
        
        self.mock_connection.fetch_all.assert_called_once()
        self.assertEqual(result, expected_result)

    def test_first(self):
        expected_result = {"id": 1, "name": "John"}
        self.mock_connection.fetch_one.return_value = expected_result
        
        result = self.builder.first()
        
        self.mock_connection.fetch_one.assert_called_once()
        self.assertEqual(result, expected_result)
        self.assertIsNone(self.builder._limit)  # 元の状態に戻されていることを確認

    def test_insert_single_row(self):
        data = {"name": "John", "email": "john@example.com"}
        self.mock_connection.execute.return_value = True
        
        result = self.builder.insert(data)
        
        self.mock_connection.execute.assert_called_once()
        args = self.mock_connection.execute.call_args[0]
        self.assertIn("INSERT INTO users", args[0])
        self.assertIn("email, name", args[0])  # カラム名
        self.assertIn("?, ?", args[0])  # プレースホルダ
        self.assertEqual(args[1], ["john@example.com", "John"])  # バインディング
        self.assertTrue(result)

    def test_insert_multiple_rows(self):
        data = [
            {"name": "John", "email": "john@example.com"},
            {"name": "Jane", "email": "jane@example.com"}
        ]
        self.mock_connection.execute.return_value = True
        
        result = self.builder.insert(data)
        
        self.mock_connection.execute_many.assert_called_once()
        args = self.mock_connection.execute_many.call_args[0]
        self.assertIn("INSERT INTO users", args[0])
        self.assertIn("(?, ?)", args[0])  # 複数セットのプレースホルダ
        self.assertEqual(args[1], [['john@example.com', 'John'], ['jane@example.com', 'Jane']])  # バインディング
        self.assertTrue(result)

    def test_update(self):
        self.builder.where("id", 1)
        self.mock_connection.execute.return_value = True
        
        result = self.builder.update({"name": "Updated", "email": "updated@example.com"})
        
        self.mock_connection.execute.assert_called_once()
        args = self.mock_connection.execute.call_args[0]
        self.assertIn("UPDATE users SET", args[0])
        self.assertIn("name = ?, email = ?", args[0])
        self.assertIn("WHERE id = ?", args[0])
        self.assertEqual(args[1], ["Updated", "updated@example.com", 1])  # updateのバインディング + whereのバインディング
        self.assertTrue(result)

    def test_delete(self):
        self.builder.where("id", 1)
        self.mock_connection.execute.return_value = True
        
        result = self.builder.delete()
        
        self.mock_connection.execute.assert_called_once()
        args = self.mock_connection.execute.call_args[0]
        self.assertEqual(args[0], "DELETE FROM users WHERE id = ?")
        self.assertEqual(args[1], [1])  # バインディング
        self.assertTrue(result)

    def test_build_select_query(self):
        # 複雑なクエリを構築
        query = self.builder \
            .select("id", "name") \
            .where("active", True) \
            .where("age", ">", 30) \
            .join("contacts", "users.id", "=", "contacts.user_id") \
            .group_by("department_id") \
            .having("count", ">", 5) \
            .order_by("name", "asc") \
            .limit(10) \
            .offset(5)
        
        sql, bindings = query._build_select_query()
        
        # 各部分をチェック
        self.assertIn("SELECT id, name FROM users", sql)
        self.assertIn("JOIN contacts ON users.id = contacts.user_id", sql)
        self.assertIn("WHERE active = ? AND age > ?", sql)
        self.assertIn("GROUP BY department_id", sql)
        self.assertIn("HAVING count > ?", sql)
        self.assertIn("ORDER BY name asc", sql)
        self.assertIn("LIMIT 10", sql)
        self.assertIn("OFFSET 5", sql)
        
        # バインディング確認
        self.assertEqual(bindings, [True, 30, 5])

    def test_to_sql(self):
        query = self.builder.where("name", "John")
        sql, bindings = query.to_sql()
        
        self.assertEqual(sql, "SELECT * FROM users WHERE name = ?")
        self.assertEqual(bindings, ["John"])

    def test_dump(self):
        # 標準出力をキャプチャするためのパッチ
        with patch('builtins.print') as mock_print:
            self.builder.where("name", "John").dump()
            
            # printが呼ばれたことを確認
            self.assertEqual(mock_print.call_count, 2)
            # 1回目の呼び出し (SQL)
            self.assertIn("SELECT * FROM users WHERE name = ?", mock_print.call_args_list[0][0][1])
            # 2回目の呼び出し (バインディング)
            self.assertEqual(["John"], mock_print.call_args_list[1][0][1])


if __name__ == '__main__':
    unittest.main()