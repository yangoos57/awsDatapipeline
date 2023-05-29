from typing import List, Any, Callable
from sqlalchemy.orm import Session
import db.schemas as schema
import db.table as table
import pandas as pd


def load_book_info(db: Session, isbn: List[str]) -> List[schema.BookInfoSchemas]:
    """사용자 검색 결과에 따른 도서 정보 제공"""
    data = db.query(table.BookInfo).filter(table.BookInfo.isbn13.in_(isbn)).all()
    result = [v.__dict__ for v in data]
    [v.pop("_sa_instance_state", None) for v in result]

    return result


def check_books_in_selected_lib(
    db: Session, isbn: List[str], lib_name: List[str]
) -> List[schema.LibBookSchemas]:
    """선택한 도서관 내 추천 장서 보유 여부 제공"""
    data = (
        db.query(table.LibBooks.isbn13, table.LibBooks.lib_name)
        .filter(table.LibBooks.isbn13.in_(isbn))
        .filter(table.LibBooks.lib_name.in_(lib_name))
        .all()
    )
    return data


def load_lib_isbn(db: Session, lib_name: List[str]) -> List[schema.LibBookSchemas]:
    """선택한 도서관의 장서 ISBN 제공"""
    data = db.query(table.LibBooks.isbn13).filter(table.LibBooks.lib_name.in_(lib_name)).all()
    return [v[0] for v in data]


def update_db(db: Session, table_name: str, features: Any):
    """매월 도서정보 업데이트 시 활용"""
    table_dict = dict(lib_books=table.LibBooks, book_info=table.BookInfo)
    current_table = table_dict[table_name]
    columns_table = [c.key for c in current_table.__table__.columns]

    duplicated_book_info = pd.DataFrame(features.dict(), columns=columns_table)
    pure_book_info_df = _eleminate_duplicate(db, current_table, duplicated_book_info)

    if pure_book_info_df.empty:
        return None
    else:
        features = pure_book_info_df.to_dict("records")
        db.bulk_insert_mappings(current_table, features)
        db.commit()
        return features


def _eleminate_duplicate(db: Session, current_table: Callable, df: pd.DataFrame) -> pd.DataFrame:
    """ISBN13을 기준으로 중복된 값 제거"""
    # 수정필요:LibBook 업데이트 시 isbn13 및 도서관명을 고려해야함
    existing_isbn_in_db_tuple = (
        db.query(current_table.isbn13).filter(current_table.isbn13.in_(df.isbn13.tolist())).all()
    )
    existing_isbn_in_db = [v[0] for v in existing_isbn_in_db_tuple]
    non_existing_data_in_db = df[~df["isbn13"].isin(existing_isbn_in_db)]
    return non_existing_data_in_db


def upload_data_when_init(db: Session):
    """프로그램 처음 시작 시 db 세팅"""
    table_dict = dict(lib_books=table.LibBooks, book_info=table.BookInfo)
    for name, current_table in table_dict.items():
        row_count = db.query(current_table).count()
        if row_count == 0:
            print(f"{name} is empty set...")
            data = pd.read_parquet(f"db/data/{name}.parquet")
            print(f"Uploading {name}_data")
            features = data.to_dict("records")
            db.bulk_insert_mappings(current_table, features)
            db.commit()
            print(f"finsihed!!\n")


def insert_user_choice(db: Session, user_id, isbn13):
    row = table.UserChoice(user_id=user_id, isbn13=isbn13)
    db.add(row)
    db.commit()
    return row
