from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from configs import Deployment
from uuid import uuid4
from search import BookSearcher
from datetime import datetime
from db.dynamodb import put_item
from boto3 import resource

# Env
env = Deployment()

# Load BookSearcher Class
book_searcher = BookSearcher()

# Connect to Mysql
SQLALCHEMY_DATABASE_URL = f"{env.db_dir}/{env.db_name}"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Connect to DynamoDB
dynamo_db_table = resource("dynamodb").Table("dodo-dynamo-db")
print("Session Connected!")


def handler(event, context):
    print(event)
    try:
        book_df = book_searcher.create_book_recommandation_df(session, event).to_dict("records")
        resp = True
    except KeyError as e:
        book_df = {}
        resp = False

    # Add DynamoDB Items
    query_id = str(uuid4())
    dynamodb_item = dict(
        query_id=query_id,
        query_date=datetime.today().date().strftime("%Y-%m-%d"),
        user_search=event["user_search"],
        selected_lib=event["selected_lib"],
        isbn13_list=list(map(lambda x: x["isbn13"], book_df)),
    )
    put_item(Item=dynamodb_item, table=dynamo_db_table)

    return {
        "query_id": query_id,
        "user_search": event["user_search"],
        "selected_lib": event["selected_lib"],
        "response": resp,
        "result": book_df,
    }


# if __name__ == "__main__":
#     test_data = {"user_search": ["도커"], "selected_lib": ["양천"]}
#     result = handler(event=test_data, context="t")
