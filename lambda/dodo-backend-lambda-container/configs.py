import os

url = "dodomoards.ccoalf3s8d7c.ap-northeast-2.rds.amazonaws.com"
user = os.getenv("db_user")
password = os.getenv("password")


class Deployment:
    def __init__(self) -> None:
        self._db_dir = f"mysql+pymysql://{user}:{password}@{url}:3306"
        # db_dir = f"mysql+pymysql://{user}@{url}:3306"
        self._db_name = "dodomoa_db"
        self._columns = [
            "isbn13",
            "bookname",
            "authors",
            "publisher",
            "class_no",
            "reg_date",
            "bookImageURL",
        ]

    @property
    def db_dir(self):
        return self._db_dir

    @property
    def db_name(self):
        return self._db_name

    @property
    def api_auth_key(self):
        return self._api_auth_key

    @property
    def columns(self):
        return self._columns


class Test:
    def __init__(self) -> None:
        self._db_dir = f"mysql+pymysql://{user}:{url}:3306"
        self._db_name = "dodomoa_test"

    @property
    def db_dir(self):
        return self._db_dir

    @property
    def db_name(self):
        return self._db_name
