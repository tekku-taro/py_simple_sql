# Py Simple SQL

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Py Simple SQL** は、Laravel の DB ファサードを参考に開発された、直感的で流れるようなインターフェースで SQL クエリを作成・実行できる Python ライブラリです。

## 🎯 目的

*   複雑な ORM の学習コストなしに、データベース操作をシンプルに記述したい。
*   SQL を直接記述するよりも安全かつ表現力豊かにクエリを組み立てたい。
*   Sqlite, MySQL, PostgreSQL といった複数のデータベースを透過的に扱いたい。

## ✨ 特徴

*   **直感的なチェーンメソッド:** メソッドをつなげていくことで、読みやすく、書きやすいクエリを構築できます。
*   **複数データベース対応:** Sqlite, MySQL, PostgreSQL をサポート。データベースごとの SQL 方言の違いをある程度吸収できます。
*   **安全性:** プレースホルダとバインディングを使用し、SQL インジェクションのリスクを自動的に軽減します。
*   **柔軟性:** クエリビルダだけでなく、生 SQL の実行もサポートします。
*   **トランザクション管理:** `with` 文を使ったシンプルなトランザクション管理を提供します。
*   **デバッグ支援:** 生成される SQL とバインディングパラメータを簡単に確認できます。

## 💾 対応データベース

*   SQLite
*   MySQL
*   PostgreSQL

## 📦 インストール


1.  **ソースコードの取得:**
    *   このリポジトリをクローンするか、ZIP ファイルとしてダウンロードします。
    *   `git clone https://github.com/tekku-taro/py_simple_sql.git` 

2.  **`dbquery` ディレクトリのコピー:**
    *   取得したソースコードの中にある `dbquery` ディレクトリを、あなたのプロジェクト内の適切な場所（例: プロジェクトルートや `lib` フォルダなど）にコピーします。

3.  **インポート:**
    *   コピーした `dbquery` ディレクトリから、必要なクラス（主に `DB` クラス）をインポートして使用します。

    ```python
    # 例: プロジェクトルートに dbquery ディレクトリをコピーした場合
    from dbquery.db import DB
    # from dbquery.query_builder import QueryBuilder # 必要に応じて

    # --- DBクラスの利用 ---
    config = { ... } # データベース設定
    db = DB(config)

    # users = db.table("users").get()
    # ...
    ```


## 🚀 使い方
### 接続設定
まず、データベースへの接続設定を辞書形式で作成し、DB クラスのインスタンス作成時に引数で渡します。 

```python
import os
from dbquery.db import DB
from dotenv import load_dotenv

# .envファイルなどから設定を読み込む
load_dotenv()

config = {
    "driver": os.getenv("DB_CONNECTION", "sqlite"),
    "database": os.getenv("DB_DATABASE", ":memory:"), # SQLite の場合ファイルパス or :memory:
    # --- MySQL/PostgreSQL の場合に必要な設定 ---
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)), # 必要に応じて適切なポート番号に
    "user": os.getenv("DB_USERNAME", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
}
# None の値を持つキーを config から削除
config = {k: v for k, v in config.items() if v is not None}

# DBインスタンスの作成
db = DB(config)

# --- これ以降、dbオブジェクトを使ってクエリを実行 ---

# 使い終わったら接続を切断 (省略可能、デストラクタで自動的に呼ばれます)
# db.disconnect()
```

### SELECT クエリ
#### 基本的な取得
.table() でテーブル名を指定し、.get() で全件取得、.first() で最初の1件を取得します。

```python
# usersテーブルの全レコードを取得 (List[Dict[str, Any]])
all_users = db.table("users").get()

# usersテーブルの最初の1レコードを取得 (Dict[str, Any] or None)
user = db.table("users").where("id", 1).first()
if user:
    print(user["email"])
```

#### カラム指定
.select() で取得するカラムを指定できます。省略した場合は * (全カラム) になります。

```python
# name と email カラムのみ取得
users = db.table("users").select("name", "email").get()
```

#### Where 句
.where() で条件を指定します。演算子を省略した場合は = とみなされます。

```python
# id が 1 のユーザーを取得
user = db.table("users").where("id", 1).first()

# votes が 10 より大きいユーザーを取得
active_users = db.table("users").where("votes", ">", 10).get()

# 複数の条件 (AND)
user = db.table("users").where("name", "John").where("active", True).first()

# WHERE IN
users = db.table("users").where_in("id", [1, 3, 5]).get()

# WHERE NOT IN
users = db.table("users").where_not_in("status", ["pending", "failed"]).get()
```

#### Join 句
.join(), .left_join(), .right_join() でテーブルを結合します。

```python
# users と contacts テーブルを内部結合
users_with_contacts = db.table("users") \
    .join("contacts", "users.id", "=", "contacts.user_id") \
    .select("users.name", "contacts.phone") \
    .get()

# users と orders テーブルを左結合
users_with_orders = db.table("users") \
    .left_join("orders", "users.id", "=", "orders.user_id") \
    .get()
```

#### Order By, Limit, Offset
ソート順、取得件数、開始位置を指定します。

```python
# 作成日時の降順で最初の10件を取得
recent_users = db.table("users") \
    .order_by("created_at", "desc") \
    .limit(10) \
    .get()

# ページネーション (2ページ目、1ページあたり15件)
page = 2
per_page = 15
users = db.table("users") \
    .order_by("id") \
    .offset((page - 1) * per_page) \
    .limit(per_page) \
    .get()
```

#### 集計クエリ
.count(), .max(), .min(), .avg(), .sum() を使用して集計結果を取得します。

```python
# アクティブユーザー数をカウント
active_user_count = db.table("users").where("active", True).count()
print(f"Active users: {active_user_count}")

# 最高得点を取得
max_votes = db.table("users").max("votes")
print(f"Max votes: {max_votes}")

# 平均価格を取得
average_price = db.table("orders").where("status", "completed").avg("price")
print(f"Average price: {average_price}")
```

#### 存在チェック
.exists() で条件に一致するレコードが存在するかどうかを bool で返します。

```python
email_exists = db.table("users").where("email", "test@example.com").exists()

if email_exists:
    print("このメールアドレスは既に登録されています。")
else:
    # 登録処理など
    pass
```

### INSERT クエリ
.insert() に辞書または辞書のリストを渡してデータを挿入します。

```python
# 単一レコードの挿入
success = db.table("users").insert({
    "name": "Kayla",
    "email": "kayla@example.com",
    "votes": 0
})

# 複数レコードの挿入
success = db.table("users").insert([
    {"name": "John", "email": "john@example.com"},
    {"name": "Jane", "email": "jane@example.com"}
])
```

### UPDATE クエリ
.where() で更新対象を指定し、.update() に更新内容の辞書を渡します。

```python
# id が 1 のユーザーの votes を更新
success = db.table("users").where("id", 1).update({"votes": 10})

# 複数のカラムを更新
success = db.table("users").where("email", "old@example.com").update({
    "email": "new@example.com",
    "updated_at": "2023-10-27 10:00:00" # 必要に応じて日時を生成
})
```

DELETE クエリ
.where() で削除対象を指定し、.delete() を呼び出します。


```python
# votes が 100 より大きいユーザーを削除
success = db.table("users").where("votes", ">", 100).delete()

# 特定のユーザーを削除
success = db.table("users").where("id", 5).delete()
```

### トランザクション
with db.transaction(): ブロックを使用することで、一連の処理をアトミックに行えます。ブロック内で例外が発生すると自動的にロールバックされ、正常に終了するとコミットされます。


```python
try:
    with db.transaction():
        # ユーザーの votes を更新
        db.table("users").where("id", 1).update({"votes": 1})
        # ログテーブルに記録
        db.table("logs").insert({"action": "updated votes for user 1", "user_id": 1})
    print("トランザクション成功")
except Exception as e:
    print(f"トランザクション失敗: {e}")
    # ロールバックは自動的に行われる
```

### 生 SQL の実行
クエリビルダでは表現できない複雑なクエリや、DDL を実行したい場合は raw() や raw_execute() を使用します。


```python
# SELECT クエリ (結果を List[Dict] で取得)
results = db.raw("SELECT name, email FROM users WHERE votes > ? ORDER BY name", [10])
for row in results:
    print(row)

# INSERT/UPDATE/DELETE/DDL など (成功したかどうかの bool を返す)
success = db.raw_execute("UPDATE users SET votes = votes + 1 WHERE active = ?", [True])

# DDL の実行例
try:
    db.raw_execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("Table 'logs' created or already exists.")
except Exception as e:
    print(f"Failed to create table: {e}")
```

## 🛠️ デバッグ
### to_sql()
クエリを実行せずに、生成される SQL 文とバインディングパラメータを確認できます。


```python
sql, bindings = db.table("users").where("votes", ">", 100).to_sql()

print("SQL:", sql)
# 出力例: SQL: SELECT * FROM users WHERE votes > ?
print("Bindings:", bindings)
# 出力例: Bindings: [100]
```

### dump()
to_sql() と同様ですが、SQL とバインディングを直接 print します。デバッグ中に手軽に使えます。


```python
db.table("users").where("votes", ">", 100).dump()
# 出力例:
# SQL: SELECT * FROM users WHERE votes > ?
# Bindings: [100]
```

## 🔐 セキュリティ
Py Simple SQL は、SQL インジェクション攻撃を防ぐために、常にプレースホルダとパラメータバインディングを使用します。.where() や .insert(), .update() に渡された値は、直接 SQL 文字列に埋め込まれるのではなく、データベースドライバによって安全に処理されます。

ユーザー入力データを使って、生 SQL を実行する raw() や raw_execute() を使用する場合も、パラメータは必ず bindings 引数で渡すようにしてください。


```python
# 安全な例
user_id = 10
results = db.raw("SELECT * FROM users WHERE id = ?", [user_id])

# 危険な例 (絶対に避けるべき)
# user_id_input = input("Enter user ID: ")
# results = db.raw(f"SELECT * FROM users WHERE id = {user_id_input}") # SQLインジェクションの脆弱性！
```


## 📜 ライセンス
Py Simple SQL は MIT ライセンスのもとで提供されます。