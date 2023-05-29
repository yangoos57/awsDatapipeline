from fastapi import APIRouter, Depends, BackgroundTasks
from db.database import get_db, Base, engine
from db.schemas import BookInfoSchemas
from sqlalchemy.orm import Session
from .search import BookSearcher
from routers.schemas import *
from typing import Dict
from uuid import uuid4
import db.cruds as query
from logs.utils import make_monitoring_logger

router = APIRouter()
Base.metadata.create_all(bind=engine)
query.upload_data_when_init(db=next(get_db()))
book_searcher = BookSearcher()
log = make_monitoring_logger(__name__)


@router.get("/health")
def health() -> Dict[str, str]:
    return {"health": "ok"}


@router.post("/test")
def test(data: Data, background_task: BackgroundTasks, db: Session = Depends(get_db)):
    request_data = data.dict()

    return {"text": query.load_lib_isbn(db, request_data["selected_lib"])}


@router.post("/predict")
def predict(data: Data, background_task: BackgroundTasks, db: Session = Depends(get_db)):
    uuid = str(uuid4())
    request_data = data.dict()
    user_keys = request_data["user_search"]
    log.info(f"키워드 : {user_keys}")

    try:
        book_df = book_searcher.create_book_recommandation_df(db, request_data).to_dict("records")
        resp = True
    except KeyError as e:
        log.error(e)
        book_df = {}
        resp = False

    return {
        "user_search": request_data["user_search"],
        "selected_lib": request_data["selected_lib"],
        "response": resp,
        "table_id": uuid,
        "result": book_df,
    }


@router.post("/update/book-info")
def test(data: BookInfoSchemas, background_task: BackgroundTasks, db: Session = Depends(get_db)):
    return query.update_db(db, "book_info", data)


@router.post("/update/lib-books")
def test(data: LibBookSchemas, background_task: BackgroundTasks, db: Session = Depends(get_db)):
    return query.update_db(db, "lib_books", data)
