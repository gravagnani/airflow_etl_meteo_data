from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import pandas as pd
import logging


@task(task_id="read_stores")
def read_stores():
    # Estract from table
    postgres_hook = PostgresHook(postgres_conn_id="postgres", schema="airflow")
    df = postgres_hook.get_pandas_df(
        sql=f"""
            SELECT location_id,
                   latitude_id,
                   longitude_id
            FROM data.stores
            WHERE load='Y'
        """,
        parameters={},
    )

    stores = df.values.tolist()

    logging.info(stores)

    return stores
