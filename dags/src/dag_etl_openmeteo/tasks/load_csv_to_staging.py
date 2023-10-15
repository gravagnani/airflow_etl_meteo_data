from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import pandas as pd
import logging


@task(task_id="load_csv_to_staging", weight_rule="upstream")
def load_csv_to_staging(data):
    csv_filename = data["filename"]
    store_id = data["store_id"]

    logging.info(f"Loading to Staging {csv_filename}")

    df = pd.read_csv(
        f"./dags/stage/etl_openmeteo/csv/transformed/{csv_filename}",
        sep=";",
        quoting=csv.QUOTE_NONNUMERIC,
        quotechar='"',
        encoding="utf-8",
        dtype=str,
    )
    postgres_hook = PostgresHook(postgres_conn_id="postgres", schema="airflow")
    extract_table = f"e_openmeteo_data_{store_id}"
    df.to_sql(
        name=extract_table,
        con=postgres_hook.get_sqlalchemy_engine(),
        schema="staging",
        index=False,
        if_exists="replace",
        chunksize=1000,
    )

    return {"extract_table": extract_table, "store_id": store_id}
