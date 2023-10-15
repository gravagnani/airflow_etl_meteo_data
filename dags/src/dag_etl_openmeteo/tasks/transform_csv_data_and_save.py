from airflow.decorators import task
import logging
import os
import pandas as pd
import csv


@task(task_id="transform_csv_data_and_save", weight_rule="upstream")
def transform_csv_data_and_save(data):
    csv_filename = data["filename"]
    store_id = data["store_id"]

    logging.info(f"Transforming csv {csv_filename}")

    base_dir_extracted = "./dags/stage/etl_openmeteo/csv/extracted/"

    df_head = pd.read_csv(f"{base_dir_extracted}/{csv_filename}", nrows=1)
    df_data = pd.read_csv(f"{base_dir_extracted}/{csv_filename}", skiprows=2)
    df = pd.merge(df_head, df_data, how="cross")

    base_dir_transformed = "./dags/stage/etl_openmeteo/csv/transformed/"
    out_filename = f"{csv_filename}"

    if not os.path.exists(base_dir_transformed):
        os.makedirs(base_dir_transformed)

    df.to_csv(
        f"{base_dir_transformed}/{out_filename}",
        index=False,
        sep=";",
        quoting=csv.QUOTE_NONNUMERIC,
        quotechar='"',
    )

    return {"filename": out_filename, "store_id": store_id}
