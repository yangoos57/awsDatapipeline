from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators.python import PythonOperator
from airflow.operators import email_operator
import python_operator.report as op_repo
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
year = today.year
month = today.month
day = today.day
date_time = today.strftime("%Y-%m-%d")


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2023, 5, 19),
    tags=["example"],
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="generate_query_to_extract_data_from_redshift",
        python_callable=op_query.extract_data_for_reporting,
        op_args=[date_time],
    )

    task2 = RedshiftDataOperator(
        task_id="extract_data_for_reporting_from_redshift",
        cluster_identifier="dodo-redshift",
        database="dev",
        db_user="admin",
        sql="{{ ti.xcom_pull(task_ids='generate_query_to_extract_data_from_redshift', key='query') }}",
        wait_for_completion=True,
    )

    task3 = PythonOperator(
        task_id="generate_Report",
        python_callable=op_repo.generate_report,
        op_args=[date_time],
    )

    # Send email confirmation
    task4 = email_operator.EmailOperator(
        task_id="email_to_all_staff",
        to=["679oose@gmail.com"],
        subject=f"{date_time} 앱 이용 현황",
        html_content="<p>test</p>",
        # files=[f"../data/report/{date_time}/report_{date_time}.html"],
    )

    task1 >> task2 >> task3 >> task4
    # task3 >> task4
