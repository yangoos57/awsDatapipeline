from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import CHAR, INTEGER
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column
import os

# env
user = os.getenv("db_user")
password = os.getenv("password")
url = "dodomoards.ccoalf3s8d7c.ap-northeast-2.rds.amazonaws.com"

###############
# DB Session using Sqlalchemy.orm
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{user}:{password}@{url}:3306/dodomoa_db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
print("Session Connected!")

Base = declarative_base()


class UserChoice(Base):
    __tablename__ = "user_choice"
    id = Column(INTEGER, primary_key=True)
    user_id = Column(CHAR(10))  # user-1234
    query_id = Column(CHAR(40))
    isbn13 = Column(CHAR(13))


def insert_user_choice(user_id, query_id, isbn13):
    row = UserChoice(user_id=user_id, query_id=query_id, isbn13=isbn13)
    session.add(row)
    session.commit()
    return row


###############
