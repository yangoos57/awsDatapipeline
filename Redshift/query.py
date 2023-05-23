import boto3
import pytz
from datetime import datetime

tz = pytz.timezone("Asia/Tokyo")
today = datetime.now(tz)
date_time = today.strftime("%Y-%m-%d")


def update_table_in_redshift(date_time: str, **kwargs):
    year, month, day = date_time.split("-")
    """
    Transfer S3 -> Redshift
    use Copy Command
    """

    # load s3
    client = boto3.client("s3")
    bucket = "dodomoabucket"

    # file name
    file_name_dict = dict()

    # extract rds name
    prefix = f"dodo-glue/dodo-rds/rds-raw/{year}/{month}/{day}"
    rds_db = client.list_objects_v2(Bucket=bucket, Prefix=prefix)["Contents"]
    rds_db = sorted(rds_db, key=lambda x: x["LastModified"], reverse=True)
    file_name_dict["rds_db"] = rds_db[0]["Key"]

    # extract dynamo_db name
    prefix = f"dodo-glue/dodo-dynamo/{year}/{month}/{day}"
    db_list = client.list_objects_v2(Bucket=bucket, Prefix=prefix)["Contents"]

    def custom_fc(x):
        if ".parquet" in x["Key"] and ".crc" not in x["Key"]:
            return x["Key"]

    db_list = filter(lambda x: custom_fc(x), db_list)
    db_list = sorted(db_list, key=lambda x: x["LastModified"], reverse=True)
    db_list = list(map(lambda x: x["Key"], db_list))

    for name in ["failed_user_search", "isbn_13", "selected_lib", "success_user_search"]:
        for db_name in db_list:
            if name in db_name:
                file_name_dict[name] = db_name

    query = f"""
-- Create the new book_info table with the desired schema
CREATE TABLE IF NOT EXISTS book_info (
    isbn13 VARCHAR(512),
    bookname VARCHAR(1024),
    authors VARCHAR(128),
    publisher VARCHAR(128),
    class_no VARCHAR(128),
    reg_date VARCHAR(128),
    bookImageURL VARCHAR(128)
);

-- Copy data into the book_info table from the specified S3 file
COPY book_info
FROM 's3://dodomoabucket/dodo-glue/dodo-book/book_info.parquet'
IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
PARQUET;


-- Create the new user_choice table with the desired schema
CREATE TABLE IF NOT EXISTS user_choice (
    id INT,
    query_id VARCHAR(512),
    isbn13 VARCHAR(13),
    user_id VARCHAR(10)
);

-- Copy data into the user_choice table from the specified S3 file
COPY user_choice
FROM 's3://dodomoabucket/{file_name_dict["rds_db"]}'
IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
PARQUET;


-- Create the new isbn_13 table with the desired schema
CREATE TABLE IF NOT EXISTS isbn_13 (
    query_id VARCHAR(40),
    isbn13 VARCHAR(512)
);

-- Copy data into the isbn_13 table from the specified S3 file
COPY isbn_13
FROM 's3://dodomoabucket/{file_name_dict["isbn_13"]}'
IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
PARQUET;


-- Create the new selected_lib table with the desired schema
CREATE TABLE IF NOT EXISTS selected_lib (
    query_id VARCHAR(40),
    lib VARCHAR(128)
);

-- Copy data into the selected_lib table from the specified S3 file
COPY selected_lib
FROM 's3://dodomoabucket/{file_name_dict["selected_lib"]}'
IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
PARQUET;


-- Create the new success_user_search table with the desired schema
CREATE TABLE IF NOT EXISTS success_user_search (
    query_id VARCHAR(40),
    keyword VARCHAR(128)
);

-- Copy data into the success_user_search table from the specified S3 file
COPY success_user_search
FROM 's3://dodomoabucket/{file_name_dict["success_user_search"]}'
IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
PARQUET;


-- Create the new failed_user_search table with the desired schema
CREATE TABLE IF NOT EXISTS failed_user_search (
    query_id VARCHAR(40),
    failed_keyword VARCHAR(128)
);

-- Copy data into the failed_user_search table from the specified S3 file
COPY failed_user_search
FROM 's3://dodomoabucket/{file_name_dict["failed_user_search"]}'
IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
PARQUET;
"""
    kwargs["ti"].xcom_push(key="query", value=query)


def extract_data_for_reporting(date_time: str, **kwargs):

    year, month, day = date_time.split("-")

    query = f"""
-- 1. total user_requests_success
    UNLOAD ('SELECT count(Distinct query_id) AS requests 
            FROM success_user_search')
    TO 's3://dodomoabucket/redshift/{year}/{month}/{day}/user_requests_success-'
    IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
    FORMAT PARQUET
    ALLOWOVERWRITE
    PARALLEL OFF;


    -- 2. total user_requests_failed
    UNLOAD ('SELECT count(Distinct query_id) AS requests 
            FROM failed_user_search')
    TO 's3://dodomoabucket/redshift/{year}/{month}/{day}/user_requests_failed-'
    IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
    FORMAT PARQUET
    ALLOWOVERWRITE
    PARALLEL OFF;

    -- 3. user_selection_per_recom
    UNLOAD ('SELECT AVG(count) AS user_select 
    FROM (
    SELECT COUNT(*) AS count
    FROM user_choice 
    GROUP BY query_id
    ) subquery;'
    )
    TO 's3://dodomoabucket/redshift/{year}/{month}/{day}/user_selection_per_recom-'
    IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
    FORMAT PARQUET
    ALLOWOVERWRITE
    PARALLEL OFF;


    -- 4. count_book
    UNLOAD ('SELECT le.isbn13, re.bookname, le.count 
        FROM (SELECT isbn13, count(*) AS count 
            FROM isbn_13
            GROUP BY isbn13
            ORDER BY count DESC
            LIMIT 100) AS le
        LEFT JOIN book_info AS re
        ON le.isbn13 = re.isbn13'
    )
    TO 's3://dodomoabucket/redshift/{year}/{month}/{day}/count_book-'
    IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
    FORMAT PARQUET
    ALLOWOVERWRITE
    PARALLEL OFF;


    -- 5. count_lib
    UNLOAD ('SELECT lib, count(*) AS count 
            FROM selected_lib 
            GROUP BY lib 
            ORDER BY count DESC'
    )
    TO 's3://dodomoabucket/redshift/{year}/{month}/{day}/count_lib-'
    IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
    FORMAT PARQUET
    ALLOWOVERWRITE
    PARALLEL OFF;


    -- 6. count_keyword
    UNLOAD ('
        SELECT keyword, count FROM (
            SELECT keyword, count(*) AS count 
            FROM success_user_search 
            GROUP BY keyword 
            LIMIT 100
        ) subquery
        ORDER BY count DESC
        '
    )
    TO 's3://dodomoabucket/redshift/{year}/{month}/{day}/count_keyword-'
    IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
    FORMAT PARQUET
    ALLOWOVERWRITE
    PARALLEL OFF;


    -- 7.count_query_user
    UNLOAD ('
        SELECT user_id, count FROM (
            SELECT user_id, COUNT(DISTINCT query_id) AS count
            FROM user_choice 
            GROUP BY user_id
            LIMIT 100
        ) subquery
        ORDER BY count DESC
        '
    )
    TO 's3://dodomoabucket/redshift/{year}/{month}/{day}/count_query_user-'
    IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
    FORMAT PARQUET
    ALLOWOVERWRITE
    PARALLEL OFF;



    -- 8.failed_keyword_extract
    UNLOAD ('
    SELECT le.failed_keyword, COUNT(*) 
    FROM failed_user_search AS le
    LEFT JOIN success_user_search AS ri
    ON le.failed_keyword = ri.keyword
    WHERE ri.keyword IS NULL
    GROUP BY le.failed_keyword;'
    )
    TO 's3://dodomoabucket/redshift/{year}/{month}/{day}/failed_keyword_extract-'
    IAM_ROLE 'arn:aws:iam::965780743476:role/service-role/AmazonRedshift-CommandsAccessRole-20230516T161213'
    FORMAT PARQUET
    ALLOWOVERWRITE
    PARALLEL OFF;
"""
    kwargs["ti"].xcom_push(key="query", value=query)
