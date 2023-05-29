from pydantic import BaseModel
from typing import List


class Data(BaseModel):
    user_search: List[str]
    selected_lib: List[str]


class BookInfoSchemas(BaseModel):
    isbn13: List[str]
    bookname: List[str]
    authors: List[str]
    publisher: List[str]
    class_no: List[str]
    reg_date: List[str]
    bookImageURL: List[str]


class LibBookSchemas(BaseModel):
    isbn13: List[str]
    lib_name: List[str]
