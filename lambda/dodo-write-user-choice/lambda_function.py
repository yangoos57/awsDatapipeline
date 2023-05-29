import json
from db import insert_user_choice


def lambda_handler(event, context):

    insert_user_choice(event["user_id"], event["query_id"], event["isbn13"])

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
