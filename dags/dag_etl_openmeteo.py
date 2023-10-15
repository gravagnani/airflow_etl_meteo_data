from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task_group

from src.dag_etl_openmeteo.tasks.start import start
from src.dag_etl_openmeteo.tasks.end import end
from src.dag_etl_openmeteo.tasks.read_stores import (
    read_stores,
)
from src.dag_etl_openmeteo.tasks.fetch_data_and_save_csv import (
    fetch_data_and_save_csv,
)
from src.dag_etl_openmeteo.tasks.transform_csv_data_and_save import (
    transform_csv_data_and_save,
)
from src.dag_etl_openmeteo.tasks.load_csv_to_staging import (
    load_csv_to_staging,
)
from src.dag_etl_openmeteo.tasks.transform_staging_data import (
    transform_staging_data,
)
from src.dag_etl_openmeteo.tasks.merge_into_target import (
    merge_into_target,
)


default_args = {
    "owner": "gravagnani",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    dag_id="dag_etl_openmeteo",
    description="An ETL to import Open Meteo Data",
    start_date=datetime(2023, 8, 15),
    schedule=None,
    # schedule_interval="@daily",
    catchup=False,
    concurrency=10,
    params={"start_date": "2021-07-19", "end_date": "2023-07-21"},
)
def dag_etl_openmeteo():
    @task_group(group_id="tg_etl_openmeteo_store")
    def tg_etl_openmeteo_store(tgg_store):
        # Define ETL Store Level
        @task_group(group_id="tg_extract")
        def tg_extract(tg_store):
            t1 = fetch_data_and_save_csv(store=tg_store)
            t2 = transform_csv_data_and_save(t1)
            t3 = load_csv_to_staging(t2)

            return t3

        @task_group(group_id="tg_transform")
        def tg_transform(tg_extract_data):
            t4 = transform_staging_data(tg_extract_data)

            return t4

        @task_group(group_id="tg_load")
        def tg_load(tg_transform_data):
            merge_into_target(tg_transform_data)

            pass

        run_extracted_table = tg_extract(tgg_store)
        run_transformed_table = tg_transform(run_extracted_table)
        run_tg_load = tg_load(run_transformed_table)

        run_extracted_table >> run_transformed_table >> run_tg_load

    stores = read_stores()
    tg_etl_openmeteo_store_group = tg_etl_openmeteo_store.expand(tgg_store=stores)

    start() >> stores >> tg_etl_openmeteo_store_group >> end()


dag_etl_openmeteo()
