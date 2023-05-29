from sqlalchemy.dialects.mysql import VARCHAR, CHAR, INTEGER, DATE
from sqlalchemy import Column
from db.database import Base


class BookInfo(Base):
    __tablename__ = "book_info"
    isbn13 = Column(CHAR(13), primary_key=True, autoincrement=False)
    bookname = Column(VARCHAR(255))
    authors = Column(VARCHAR(255))
    publisher = Column(VARCHAR(255))
    class_no = Column(CHAR(12))
    reg_date = Column(DATE)
    bookImageURL = Column(VARCHAR(255))


class LibBooks(Base):
    __tablename__ = "lib_books"
    id = Column(INTEGER, primary_key=True)
    isbn13 = Column(CHAR(13), index=True)
    lib_name = Column(CHAR(3))


class UserChoice(Base):
    __tablename__ = "user_choice"
    id = Column(INTEGER, primary_key=True)
    query_id = Column(CHAR(40))
    isbn13 = Column(CHAR(13))
    user_id = Column(CHAR(10))  # user-1234
