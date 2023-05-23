from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import python_operator.functions as op_fc
import python_operator.query as op_query
from datetime import datetime
from airflow import DAG
import pytz
import os

DAG_ID = os.path.basename(__file__)
glue_job_name = "dodo-rds-user-choice-etl"
bucket_name = "aws-glue-assets-965780743476-ap-northeast-2"
role_name = "AWSGlueServiceRole"

tz = pytz.timezone("Asia/Tokyo")
today = datetime.now(tz)
date_time = today.strftime("%Y-%m-%d")
year, month, day = date_time.split("-")
dynamo_db_raw_dir = f"data/dynamo-db-raw/{year}/{month}/{day}"
op_fc.check_directory(dynamo_db_raw_dir)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2023, 5, 19),
    tags=["example"],
    catchup=False,
) as dag:

    task1_1 = PythonOperator(
        task_id="extract_dynamo_db_to_local",
        python_callable=op_fc.extract_dynamo_db_to_local,
        op_args=[dynamo_db_raw_dir],
    )
    task1_2 = PythonOperator(
        task_id="transform_dynamo_db",
        python_callable=op_fc.transform_dynamo_db,
        op_args=[dynamo_db_raw_dir, date_time],
    )

    task1_3 = BashOperator(
        task_id="extract_dynamo_db_to_s3",
        bash_command=f"aws s3 sync {os.getcwd()}/data/result/ s3://dodomoabucket/dodo-glue/dodo-dynamo/",
    )
    task2 = GlueJobOperator(
        task_id="extract_rds_to_s3",
        job_name=glue_job_name,
        script_location=f"s3://{bucket_name}/scripts/dodo-rds-user-choice-etl.py",
        s3_bucket=bucket_name,
        iam_role_name=role_name,
        create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
    )

    task3 = PythonOperator(
        task_id="generate_query_to_update_redshift",
        python_callable=op_query.generate_query_to_update_redshift,
        op_args=[date_time],
    )

    task4 = RedshiftDataOperator(
        task_id="update_redshift_table",
        cluster_identifier="dodo-redshift",
        database="dev",
        db_user="admin",
        sql="{{ ti.xcom_pull(task_ids='generate_query_to_update_redshift', key='query') }}",
        wait_for_completion=True,
    )
    task1_1 >> task1_2 >> task1_3
    [task1_3, task2] >> task3 >> task4
