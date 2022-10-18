from airflow import DAG
from airflow import models
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

PROJECT_ID= models.Variable.get("PROJECT_ID")
BUCKET_NAME = 'odk_test/SIMS_parquet'
STAGING_DATASET = "SIMS_test"
LOCATION = "eu"
gcp_conn = "google_cloud_default"

def my_function():
    import sys
    sys.path.append('/home/airflow/gcs/dags/sims/dependencies')

    import main
    a = main.submissions_to_storage()

    return a

default_args = {
    'owner': 'SGHI',
    'depends_on_past': False,
    'start_date': datetime(2022,7,25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'schedule_interval': '@daily'
  }

with DAG("sims",default_args=default_args) as dag:

    t1 = PythonOperator(
        task_id='submissions',
        python_callable=my_function,
        dag=dag
        )

    t2 = GCSToBigQueryOperator(
        task_id = 'S_1A',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_1A.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_1Atest',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t3 = GCSToBigQueryOperator(
        task_id = 'S_1B',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_1B.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_1Btest',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t4 = GCSToBigQueryOperator(
        task_id = 'S_1C',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_1C.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_1Ctest',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t5 = GCSToBigQueryOperator(
        task_id = 'S_2A',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_2A.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_2Atest',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t6 = GCSToBigQueryOperator(
        task_id = 'S_2B',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_2B.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_2Btest',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t7 = GCSToBigQueryOperator(
        task_id = 'S_3A',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_3A.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_3Atest',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t8 = GCSToBigQueryOperator(
        task_id = 'S_3B',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_3B.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_3Btest',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t9 = GCSToBigQueryOperator(
        task_id = 'S_4A',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_4A.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_4Atest',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t10 = GCSToBigQueryOperator(
        task_id = 'S_4B',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_4B.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_4Btest',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t11 = GCSToBigQueryOperator(
        task_id = 'S_5',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_5.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_5test',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t12 = GCSToBigQueryOperator(
        task_id = 'S_6',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_6.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_6test',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t13 = GCSToBigQueryOperator(
        task_id = 'S_7',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_7.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_7test',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t14 = GCSToBigQueryOperator(
        task_id = 'S_8',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_8.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_8test',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t15 = GCSToBigQueryOperator(
        task_id = 'S_9',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_9.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_9test',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t16 = GCSToBigQueryOperator(
        task_id = 'S_10A',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_10A.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_10Atest',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t17 = GCSToBigQueryOperator(
        task_id = 'S_10B',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['S_10B.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.S_10Btest',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t18 = GCSToBigQueryOperator(
        task_id = 'big_table',
        gcp_conn_id = gcp_conn,
        bucket = BUCKET_NAME,
        source_objects = ['big_table.parquet'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.big_table',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    t19 = BigQueryOperator(
        task_id='create_all_setids_table',
        sql=f'''
        SELECT data_SIMS_ASSESSMENT_DETAILS_SIMS_CS_ASSR_NAME as Assessor_Name,
        data_SIMS_ASSESSMENT_DETAILS_SIMS_CS_ASMT_COUNTY as County,
        data_SIMS_ASSESSMENT_DETAILS_SIMS_CS_ASMT_SUB_COUNTY as Sub_County,
        data_SIMS_ASSESSMENT_DETAILS_SIMS_CS_ASMT_FACILITY_MFL as MFL,
        data_SIMS_ASSESSMENT_DETAILS_SIMS_CS_ASMT_DATE as Assessment_Date,
        Facility, Set_Average, Expected_Average, Average_Score, Set_ID
        FROM `{PROJECT_ID}.{STAGING_DATASET}.S_*`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.all_sets_averages',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        dag=dag
    )

    t20 = BigQueryOperator(
        task_id='months_quarters',
        sql=f'''
        SELECT *, CASE
        WHEN MONTH IN ("October","November","December") THEN "Q1"
        WHEN MONTH IN ("January","February","March") THEN "Q2"
        WHEN MONTH IN ("April","May","June") THEN "Q3"
        WHEN MONTH IN ("July","August","September") THEN "Q4"
        END AS Quarter
        FROM(
        SELECT *,
        FORMAT_DATETIME("%B", DATETIME(Assessment_Date)) AS MONTH,
        FROM `{PROJECT_ID}.{STAGING_DATASET}.all_sets_averages`)
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.all_sets_averages',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        dag=dag
    )

    trigger = TriggerDagRunOperator(
        task_id ='trigger',
        trigger_dag_id='sims_transforms',
        execution_date='{{ ds }}',
        reset_dag_run=True
    )

t1 >> [t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15] >> t16 >> t17
t17 >> t18 >> t19 >> t20 >> trigger
