import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import count
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import logging
import pytz

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

logger = glueContext.get_logger()
logger.info("connection success!!")

# timeset
tz = pytz.timezone("Asia/Tokyo")

today = datetime.now(tz)
date_time = today.strftime("%Y-%m-%d")
year, month, day = date_time.split("-")
# time_string = today.strftime("%H%M%S")

# job init
spark = glueContext.spark_session
connection_mysql8_options = {
    "url": "jdbc:mysql://dodomoards.ccoalf3s8d7c.ap-northeast-2.rds.amazonaws.com:3306/dodomoa_db",
    "dbtable": "user_choice",
    "user": "admin",
    "password": "1q2w3e4r!",
    "customJdbcDriverS3Path": "s3://dodomoabucket/mysql-jar/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar",
    "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver",
}

df_mysql8 = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql", connection_options=connection_mysql8_options
)
ps_df = df_mysql8.toDF()
logger.info("loading df_mysql8 success!")

ps_df.coalesce(1).write.format("parquet").mode("overwrite").save(
    f"s3://dodomoabucket/dodo-glue/dodo-rds/rds-raw/{year}/{month}/{day}"
)

job.commit()
