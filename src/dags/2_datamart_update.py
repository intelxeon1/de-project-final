import logging
import os
import time
from datetime import datetime, timezone

import pendulum
from airflow.decorators import dag, task

from lib2 import (calc_mart)
from lib2.config import (conn_info)

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
    dag_id="datamart_update",
)
def final_project_dag():    
 
    @task(task_id="calc_mart")
    def calc_data_mart(**context):
        calc_mart(conn_info=conn_info)
    calc_data_mart()        

final_dag = final_project_dag()

if __name__ == "__main__":    
    final_dag.test(
        execution_date=datetime(year=2022, month=10, day=5, tzinfo=timezone.utc)
     )
