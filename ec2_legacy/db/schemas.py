from pydantic import BaseModel


class BookInfoSchemas(BaseModel):
    isbn13: str
    bookname: str
    authors: str
    publisher: str
    class_no: str
    reg_date: str
    bookImageURL: str

    class Config:
        orm_mode = str


class LibBookSchemas(BaseModel):
    isbn13: str
    lib_name: str

    class Config:
        orm_mode = str
