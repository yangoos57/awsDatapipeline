import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from pyspark.sql.functions import explode, size
import pytz

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()

tz = pytz.timezone("Asia/Tokyo")
today = datetime.now(tz)
date_time = today.strftime("%Y-%m-%d")
year, month, day = date_time.split("-")

dyf = glueContext.create_dynamic_frame.from_catalog(
    database="dodo-glue-db", table_name="dodo_dynamo_db"
)


df = dyf.toDF()
df.coalesce(1).write.format("parquet").save(
    f"s3://dodomoabucket/dodo-glue/dodo-dynamo/dynamo-raw/{year}/{month}/{day}"
)
logger.info("saving dynamo-raw success!")


df_isbn13 = df.select(df.query_id, explode(df["isbn13_list"])).toDF("query_id", "isbn13")
df_isbn13.coalesce(1).write.format("parquet").save(
    f"s3://dodomoabucket/dodo-glue/dodo-dynamo/isbn_13/{year}/{month}/{day}"
)
logger.info("saving isbn_13 success!")


df_selected_lib = df.select(df.query_id, explode(df["selected_lib"])).toDF("query_id", "lib")
df_selected_lib.coalesce(1).write.format("parquet").save(
    f"s3://dodomoabucket/dodo-glue/dodo-dynamo/selected_lib/{year}/{month}/{day}"
)
logger.info("saving selected_lib success!")


df_success_user_search = df.filter(size(df["isbn13_list"]) > 0)
df_success_user_search = df_success_user_search.select(
    df_success_user_search.query_id, explode(df["user_search"])
).toDF("query_id", "keyword")
df_success_user_search.coalesce(1).write.format("parquet").save(
    f"s3://dodomoabucket/dodo-glue/dodo-dynamo/success_user_search/{year}/{month}/{day}"
)
logger.info("saving success_user_search success!")


df_failed_user_search = df.filter(size(df["isbn13_list"]) == 0)
df_failed_user_search = df_failed_user_search.select(
    df_failed_user_search.query_id, explode(df_failed_user_search["user_search"])
).toDF("query_id", "failed_keyword")
df_failed_user_search.coalesce(1).write.format("parquet").save(
    f"s3://dodomoabucket/dodo-glue/dodo-dynamo/failed_user_search/{year}/{month}/{day}"
)
logger.info("saving success_user_search success!")

job.commit()
