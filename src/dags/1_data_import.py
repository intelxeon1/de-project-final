import logging
import os
import time
from datetime import datetime, timezone

import pendulum
from airflow.decorators import dag, task, task_group

from lib2 import (
    create_folder,
    delete_folder,
    download_from_s3_bucket,
    load_dwh,
    load_stg,
)
from lib2.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    BASE_DATA_PATH,
    BUCKET,
    S3_URL,
    conn_info,
)
from lib2.consts import CURRENCIES_COLUMN_FORMAT, SKIP_HEADER, TABLES

logging.basicConfig(format="[%(asctime)s]  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 10, 1, 0, 0, tz="UTC"),
    catchup=False,
    tags=[
        "final_project",
    ],
    is_paused_upon_creation=True,
    dag_id="data_import",
)
def final_project_dag():
    @task(task_id="prepare_directories")
    def prepare_directories(**context):
        log.info("START")
        logical_date: str = context["logical_date"].strftime("%Y%m%d")
        temp_dir_path = f"{BASE_DATA_PATH}/{logical_date}"
        delete_folder(temp_dir_path)
        create_folder(temp_dir_path)
        return temp_dir_path

    @task(task_id="fetch_s3_data")
    def task_fetch(data_directory_path: str):
        download_from_s3_bucket(
            S3_URL,
            BUCKET,
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            data_directory_path,
        )
        return data_directory_path

    @task(task_id="upload_staging_cur")
    def upload_cur(data_directory_path: str, **context):
        logical_date: str = context["logical_date"].strftime("%Y%m%d")

        return load_stg(
            conn_info=conn_info,
            target_table=TABLES.CURRENCIES_STG.value,
            column_format=CURRENCIES_COLUMN_FORMAT,
            skip_records=SKIP_HEADER,
            temp_postfix=logical_date,
            data_directory_path=data_directory_path,
            file_mask=TABLES.CURRENCIES_MASK.value,
            swap_and_drop=True,
        )

    @task(task_id="upload_staging_tr")
    def upload_tr(data_directory_path: str, **context):
        logical_date: str = context["logical_date"].strftime("%Y%m%d")

        return load_stg(
            conn_info=conn_info,
            target_table=TABLES.TRANSACTION_STG.value,
            column_format="",
            skip_records=SKIP_HEADER,
            temp_postfix=logical_date,
            data_directory_path=data_directory_path,
            file_mask=TABLES.TRANSACTION_MASK.value,
            swap_and_drop=True,
        )

    @task(task_id="upload_dwh_tr")
    def upload_dwh_tr(**context):
        logical_date: str = context["logical_date"].strftime("%Y%m%d")
        load_dwh(
            conn_info=conn_info,
            taret_table=TABLES.TRANSACTION_DWH.value,
            from_table=TABLES.TRANSACTION_STG.value,
            loaded_date=logical_date            
        )
    
    @task(task_id="upload_dwh_cur")
    def upload_dwh_cur(**context):
        logical_date: str = context["logical_date"].strftime("%Y%m%d")
        load_dwh(
            conn_info=conn_info,
            taret_table=TABLES.CURRENCIES_DWH.value,
            from_table=TABLES.CURRENCIES_STG.value,
            loaded_date=logical_date            
        )

    path = task_fetch(prepare_directories())
    upload_cur(path) >> upload_dwh_cur()
    upload_tr(path) >> upload_dwh_tr()
    


final_dag = final_project_dag()


if __name__ == "__main__":    
    final_dag.test(
        execution_date=datetime(year=2022, month=10, day=5, tzinfo=timezone.utc)
     )