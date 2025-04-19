from typing import Any, Dict, List, Optional, Tuple, Union
from collections import defaultdict
from dbquery.connection import Connection


class QueryBuilder:
    def __init__(self, connection:Connection):
        self.connection = connection
        self._table = None
        self._columns = ["*"]
        self._where_clauses = []
        self._bindings = []
        self._join_clauses = []
        self._order_by_clauses = []
        self._limit = None
        self._offset = None
        self._group_by = []
        self._having_clauses = []

    def table(self, table_name: str) -> 'QueryBuilder':
        """テーブル名を指定します"""
        self._table = table_name
        return self

    def select(self, *columns) -> 'QueryBuilder':
        """取得するカラムを指定します"""
        if columns:
            self._columns = columns
        return self

    def where(self, column: str, operator_or_value: Any, value: Any = None) -> 'QueryBuilder':
        """WHERE条件を追加します"""
        if value is None:
            # 2引数形式の場合、operatorは '=' とみなす
            value = operator_or_value
            operator = "="
        else:
            # 3引数形式の場合、operatorはそのまま使用
            operator = operator_or_value

        self._where_clauses.append((column, operator, value))
        self._bindings.append(value)
        return self

    def where_in(self, column: str, values: List[Any]) -> 'QueryBuilder':
        """WHERE IN条件を追加します"""
        if not values: # 値がない場合は条件を追加しない 
            return self
        
        placeholder = self.connection.get_placeholder()
        placeholders = ", ".join([placeholder] * len(values))
        self._where_clauses.append((f"{column} IN ({placeholders})", None, None))
        self._bindings.extend(values)
        return self

    def where_not_in(self, column: str, values: List[Any]) -> 'QueryBuilder':
        """WHERE NOT IN条件を追加します"""
        if not values: # 値がない場合は条件を追加しない 
            return self
        placeholder = self.connection.get_placeholder()
        placeholders = ", ".join([placeholder] * len(values))  
        self._where_clauses.append((f"{column} NOT IN ({placeholders})", None, None))
        self._bindings.extend(values)
        return self

    def order_by(self, column: str, direction: str = "asc") -> 'QueryBuilder':
        """ORDER BY条件を追加します"""
        self._order_by_clauses.append((column, direction.lower()))
        return self

    def limit(self, value: int) -> 'QueryBuilder':
        """LIMIT句を追加します"""
        self._limit = value
        return self

    def offset(self, value: int) -> 'QueryBuilder':
        """OFFSET句を追加します"""
        self._offset = value
        return self

    def join(self, table: str, first: str, operator: str, second: str) -> 'QueryBuilder':
        """INNER JOIN句を追加します"""
        return self._add_join("inner", table, first, operator, second)

    def left_join(self, table: str, first: str, operator: str, second: str) -> 'QueryBuilder':
        """LEFT JOIN句を追加します"""
        return self._add_join("left", table, first, operator, second)

    def right_join(self, table: str, first: str, operator: str, second: str) -> 'QueryBuilder':
        """RIGHT JOIN句を追加します"""
        return self._add_join("right", table, first, operator, second)

    def _add_join(self, type_: str, table: str, first: str, operator: str, second: str) -> 'QueryBuilder':
        """JOIN句を追加します（内部メソッド）"""
        self._join_clauses.append((type_, table, first, operator, second))
        return self

    def group_by(self, *columns) -> 'QueryBuilder':
        """GROUP BY句を追加します"""
        self._group_by.extend(columns)
        return self

    def having(self, column: str, operator_or_value: Any, value: Any = None) -> 'QueryBuilder':
        """HAVING条件を追加します"""
        if value is None:
            value = operator_or_value
            operator = "="
        else:
            operator = operator_or_value

        self._having_clauses.append((column, operator, value))
        self._bindings.append(value)
        return self

    def exists(self) -> bool:
        """クエリ結果が存在するかどうかをチェックします"""
        original_columns = self._columns
        self._columns = ["1"]
        sql, bindings = self._build_select_query()
        sql = f"SELECT EXISTS({sql}) as exists_flag"
        
        result = self.connection.fetch_one(sql, bindings)
        self._columns = original_columns
        
        return bool(result["exists_flag"])

    def count(self, column: str = "*") -> int:
        """レコード数を取得します"""
        return self._aggregate("COUNT", column)

    def max(self, column: str) -> Any:
        """最大値を取得します"""
        return self._aggregate("MAX", column)

    def min(self, column: str) -> Any:
        """最小値を取得します"""
        return self._aggregate("MIN", column)

    def avg(self, column: str) -> float:
        """平均値を取得します"""
        return self._aggregate("AVG", column)

    def sum(self, column: str) -> Any:
        """合計値を取得します"""
        return self._aggregate("SUM", column)

    def _aggregate(self, function: str, column: str) -> Any:
        """集計関数を実行します（内部メソッド）"""
        original_columns = self._columns
        self._columns = [f"{function}({column}) as aggregate"]
        
        sql, bindings = self._build_select_query()
        result = self.connection.fetch_one(sql, bindings)
        
        self._columns = original_columns
        return result["aggregate"]

    def get(self) -> List[Dict[str, Any]]:
        """クエリを実行して結果を取得します"""
        sql, bindings = self._build_select_query()
        return self.connection.fetch_all(sql, bindings)

    def first(self) -> Optional[Dict[str, Any]]:
        """クエリを実行して最初の1件を取得します"""
        original_limit = self._limit
        self._limit = 1
        
        sql, bindings = self._build_select_query()
        result = self.connection.fetch_one(sql, bindings)
        
        self._limit = original_limit
        return result

    def insert(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> bool:
        """
        データを挿入します。
        単一の辞書または辞書のリストを受け付けます。
        複数の行を挿入する場合、executemany を使用します。        
        """
        if not data:
            return False
            
        # 単一データか複数データかを判断
        if isinstance(data, dict):
            data = [data]
        if not data: # Check again in case the original data was an empty list
             return False
                    
        # カラム構成ごとにデータをグループ化
        grouped_data = defaultdict(list)
        for row in data:
            # frozenset をキーとして辞書をグループ化
            column_tuple = tuple(sorted(row.keys()))
            grouped_data[column_tuple].append(row)

        results = []
        # グループごとにINSERT文を実行
        for columns, rows in grouped_data.items():
            if not columns: # カラムがないデータはスキップ (またはエラー)
                print("Warning: Skipping empty row data.")
                continue

            columns_str = ", ".join(columns)
            placeholder = self.connection.get_placeholder()
            placeholders = ", ".join([placeholder] * len(columns))

            sql = f"INSERT INTO {self._table} ({columns_str}) VALUES ({placeholders})"

            if len(rows) == 1:
                # 単一挿入の場合: execute を使用
                bindings = []
                first_row = rows[0]
                bindings = [first_row[col] for col in columns]
                results.append(self.connection.execute(sql, bindings))
            else:
                # 複数挿入の場合: executemany を使用
                bindings_list = []
                for row in rows:
                    # columnsタプルの順序で値を取得
                    bindings_list.append([row[col] for col in columns])

                results.append(self.connection.execute_many(sql, bindings_list))
            
        # すべてのINSERTが成功した場合にTrueを返す
        return all(results)
    

    def update(self, data: Dict[str, Any]) -> bool:
        """データを更新します"""
        if not data:
            return False
            
        placeholder = self.connection.get_placeholder()

        # SQL生成
        set_clauses = []
        bindings = []
        
        for column, value in data.items():
            set_clauses.append(f"{column} = {placeholder}")
            bindings.append(value)
            
        sql = f"UPDATE {self._table} SET {', '.join(set_clauses)}"
        
        # WHERE句追加
        if self._where_clauses:
            sql += " WHERE " + self._build_where_clause()
            bindings.extend(self._bindings)
            
        return self.connection.execute(sql, bindings)

    def delete(self) -> bool:
        """データを削除します"""
        sql = f"DELETE FROM {self._table}"
        
        # WHERE句追加
        if self._where_clauses:
            sql += " WHERE " + self._build_where_clause()
            
        return self.connection.execute(sql, self._bindings)

    def _build_select_query(self) -> Tuple[str, List[Any]]:
        """SELECT クエリを構築します（内部メソッド）"""
        if not self._table:
            raise ValueError("Table name is required")
            
        placeholder = self.connection.get_placeholder()
        columns_str = ", ".join(self._columns)
        query = f"SELECT {columns_str} FROM {self._table}"
        
        # JOIN句追加
        if self._join_clauses:
            for join_type, table, first, operator, second in self._join_clauses:
                if join_type == "inner":
                    query += f" JOIN {table} ON {first} {operator} {second}"
                elif join_type == "left":
                    query += f" LEFT JOIN {table} ON {first} {operator} {second}"
                elif join_type == "right":
                    query += f" RIGHT JOIN {table} ON {first} {operator} {second}"
        
        # WHERE句追加
        if self._where_clauses:
            query += " WHERE " + self._build_where_clause()
        
        # GROUP BY句追加
        if self._group_by:
            query += " GROUP BY " + ", ".join(self._group_by)
        
        # HAVING句追加
        if self._having_clauses:
            having_parts = []
            for column, operator, value in self._having_clauses:
                having_parts.append(f"{column} {operator} {placeholder}")
            query += " HAVING " + " AND ".join(having_parts)
        
        # ORDER BY句追加
        if self._order_by_clauses:
            order_parts = []
            for column, direction in self._order_by_clauses:
                order_parts.append(f"{column} {direction}")
            query += " ORDER BY " + ", ".join(order_parts)
        
        # LIMIT句追加
        if self._limit is not None:
            if isinstance(self._limit, int) and self._limit >= 0:
                 query += f" LIMIT {int(self._limit)}"
            else:
                 raise ValueError("LIMIT value must be a non-negative integer.")
        
        # OFFSET句追加
        if self._offset is not None:
            if isinstance(self._offset, int) and self._offset >= 0:
                query += f" OFFSET {int(self._offset)}"
            else:
                raise ValueError("OFFSET value must be a non-negative integer.")
            
        return query, self._bindings

    def _build_where_clause(self) -> str:
        """WHERE句を構築します（内部メソッド）"""
        if not self._where_clauses:
            return ""
        
        placeholder = self.connection.get_placeholder()
        where_parts = []
        
        for clause in self._where_clauses:
            if len(clause) == 3:
                column, operator, _ = clause
                if operator is not None:
                    where_parts.append(f"{column} {operator} {placeholder}")
            else:
                # 特殊条件（WHERE IN など）の場合
                where_parts.append(clause[0])
        
        return " AND ".join(where_parts)

    def to_sql(self) -> Tuple[str, List[Any]]:
        """SQLとバインディングパラメータを返します"""
        return self._build_select_query()

    def dump(self) -> 'QueryBuilder':
        """SQLとバインディングパラメータを出力します"""
        query, bindings = self._build_select_query()
        print("SQL:", query)
        print("Bindings:", bindings)
        return self

