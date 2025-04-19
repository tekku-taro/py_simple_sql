from contextlib import contextmanager
from dbquery.connection import Connection

class TransactionManager:
    def __init__(self, connection: Connection):
        self.connection = connection

    @contextmanager
    def transaction(self):
        """トランザクションを開始し、コンテキストマネージャとして使用できるようにします"""
        try:
            self.connection.begin_transaction()
            yield
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e

