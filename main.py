import os
from dbquery.db import DB
from dotenv import load_dotenv

# サンプル使用例：
def main():
    # .env ファイルから環境変数を読み込む
    load_dotenv(override=True)
    print('DB_CONNECTION', os.getenv("DB_CONNECTION"))
    print('DB_DATABASE', os.getenv("DB_DATABASE"))

    # 環境変数から接続設定を取得
    config = {
        "driver": os.getenv("DB_CONNECTION", "sqlite"), 
        "database": os.getenv("DB_DATABASE", ":memory:"), 
        # 他のドライバに必要な設定も環境変数から取得 (必要に応じて)
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "user": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD"),
    }
    # None の値を持つキーを config から削除
    config = {k: v for k, v in config.items() if v is not None}
    
    # DB初期化
    db = DB(config)
    
    # サンプルテーブル作成

    # Sqlite
    db.raw_execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        email TEXT,
        votes INTEGER DEFAULT 0,
        active INTEGER DEFAULT 1
    )
    """)
    
    db.raw_execute("""
    CREATE TABLE IF NOT EXISTS contacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        phone TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """)
    # MySQL
    # db.raw_execute("""
    # CREATE TABLE IF NOT EXISTS users (
    #     id INT PRIMARY KEY AUTO_INCREMENT,
    #     name VARCHAR(255),
    #     email VARCHAR(255),
    #     votes INT DEFAULT 0,
    #     active INT DEFAULT 1
    # )
    # """)
    
    # db.raw_execute("""
    # CREATE TABLE IF NOT EXISTS contacts (
    #     id INT PRIMARY KEY AUTO_INCREMENT,
    #     user_id INT,
    #     phone VARCHAR(255),
    #     FOREIGN KEY (user_id) REFERENCES users(id)
    # )
    # """)
    # PostgreSQL
    # db.raw_execute("""
    # CREATE TABLE IF NOT EXISTS users (
    #     id SERIAL PRIMARY KEY,
    #     name VARCHAR(255),
    #     email VARCHAR(255),
    #     votes INT DEFAULT 0,
    #     active INT DEFAULT 1
    # )
    # """)
    
    # db.raw_execute("""
    # CREATE TABLE IF NOT EXISTS contacts (
    #     id SERIAL PRIMARY KEY,
    #     user_id INT,
    #     phone VARCHAR(255),
    #     FOREIGN KEY (user_id) REFERENCES users(id)
    # )
    # """)
    
    # データ挿入
    with db.transaction():
        db.table("users").insert([
            {"name": "John", "email": "john@example.com", "votes": 5},
            {"name": "Jane", "email": "jane@example.com", "votes": 10},
            {"name": "Bob", "email": "bob@example.com", "votes": 15, "active": 0}
        ])
        
        db.table("contacts").insert([
            {"user_id": 1, "phone": "123-456-7890"},
            {"user_id": 2, "phone": "234-567-8901"}
        ])
    
    # クエリ例
    print("全てのユーザー:")
    users = db.table("users").get()
    for user in users:
        print(user)
    
    print("\n結合クエリ:")
    result = db.table("users") \
        .join("contacts", "users.id", "=", "contacts.user_id") \
        .select("users.name", "contacts.phone") \
        .get()
    for row in result:
        print(row)
    
    print("\n集計クエリ:")
    avg_votes = db.table("users").avg("votes")
    print(f"平均投票数: {avg_votes}")
    
    count = db.table("users").where("active", 1).count()
    print(f"アクティブユーザー数: {count}")
    
    print("\n条件クエリ:")
    user = db.table("users").where("email", "john@example.com").first()
    print(user)
    
    print("\nSQL確認:")
    sql, bindings = db.table("users").where("votes", ">", 5).to_sql()
    print(f"SQL: {sql}")
    print(f"Bindings: {bindings}")
    
    # データ更新
    db.table("users").where("id", 1).update({"votes": 20})
    
    print("\n更新後のデータ:")
    user = db.table("users").where("id", 1).first()
    print(user)
    
    # データ削除
    db.table("users").where("active", 0).delete()
    
    print("\n削除後のデータ数:")
    count = db.table("users").count()
    print(f"ユーザー数: {count}")
    
    print("\nrawメソッド:")
    results = db.raw("SELECT * FROM users WHERE name = ?", ["Jane"])
    for row in results:
        print(row)

    # 接続を切断
    db.disconnect()

if __name__ == "__main__":
    main()