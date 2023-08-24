import boto3
import vertica_python
from vertica_python.vertica.cursor import Cursor
from datetime import date
import os
from .consts import (
    SQL_CREATE_TABLE,
    SQL_DROP_TABLE,
    SQL_COPY,
    SQL_SWAP_PARTITION,
    SQL_COPY_PARTITION,
    SQL_TRUNCATE_METRICS,
    SQL_UPDATE_METRICS,
)
from typing import Union
import logging
import datetime

log = logging.getLogger(__name__)


def download_from_s3_bucket(
    end_point: str, bucket: str, key_id: str, access_key: str, dir_path: str
):
    session = boto3.Session()

    s3_client = session.client(
        service_name="s3",
        endpoint_url=end_point,
        aws_access_key_id=key_id,
        aws_secret_access_key=access_key,mspa
    )

    request = s3_client.list_objects_v2(Bucket=bucket)

    for c in request["Contents"]:
        file_name = c["Key"]
        s3_client.download_file(
            Bucket=bucket, Key=file_name, Filename=f"{dir_path}/{file_name}"
        )


def delete_folder(dir_path):
    dir_already_exists = os.path.isdir(dir_path)
    if dir_already_exists:
        for file_path in os.listdir(dir_path):
            os.remove(f"{dir_path}/{file_path}")
        os.rmdir(dir_path)


def create_folder(dir_path):
    os.mkdir(dir_path)


def create_table_like(cursor: Cursor, from_table: str, target_table: str):
    drop_table(cursor, target_table)

    create_temp_table_statememt = SQL_CREATE_TABLE.format(
        new_table=target_table, from_table=from_table
    )
    cursor.execute(create_temp_table_statememt)


def drop_table(cursor: Cursor, table: str):
    drop_table_statement = SQL_DROP_TABLE.format(table_name=table)
    cursor.execute(drop_table_statement)


def load_stg(
    conn_info,
    target_table: str,
    column_format: str,
    skip_records: str,
    temp_postfix: str,
    data_directory_path: str,
    file_mask: str,
    swap_and_drop: bool,
) -> str:
    log.info("STG loading started...")
    with vertica_python.connect(**conn_info) as conn:
        loaded_table: str = ""
        cur = conn.cursor()

        temp_table = f"{target_table}_{temp_postfix}"
        create_table_like(cursor=cur, from_table=target_table, target_table=temp_table)
        files = [f for f in os.listdir(data_directory_path) if file_mask in f]
        for i, file_path in enumerate(files):
            if file_mask in file_path:
                copy_statemet = SQL_COPY.format(
                    table=temp_table,
                    column_format=column_format,
                    data_directory_path=data_directory_path,
                    file_path=file_path,
                    skip_records=skip_records,
                )
                cur.execute(copy_statemet)
                log.info(f"Loaded {i+1} from {len(files)} files.")
                loaded_table = temp_table

        if swap_and_drop:
            partition = datetime.datetime.strptime(temp_postfix, "%Y%m%d").strftime(
                "%Y-%m-%d"
            )
            swap_statement = SQL_SWAP_PARTITION.format(
                from_table=temp_table,
                start_partition=partition,
                end_partition=partition,
                target_table=target_table,
            )
            cur.execute(swap_statement)
            drop_table(cur, temp_table)
            loaded_table = target_table
    log.info("STG loading finished")
    return loaded_table


def load_dwh(
    conn_info,
    from_table: str,
    taret_table: str,
    loaded_date: str,
):
    log.info("DWH loading started...")
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        partition = datetime.datetime.strptime(loaded_date, "%Y%m%d").strftime(
            "%Y-%m-%d"
        )
        copy_statement = SQL_COPY_PARTITION.format(
            from_table=from_table,
            start_partition=partition,
            end_partition=partition,
            target_table=taret_table,
        )
        cur.execute(copy_statement)
    log.info("DWH loading finished")


def calc_mart(conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(SQL_TRUNCATE_METRICS)
        cur.execute(SQL_UPDATE_METRICS)
