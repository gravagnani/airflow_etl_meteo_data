from airflow.decorators import task
import logging
import os
import requests
from datetime import date, timedelta
from airflow.operators.python import get_current_context


@task(
    task_id="fetch_data_and_save_csv",
    weight_rule="upstream",
    retries=5,
    execution_timeout=timedelta(minutes=10),
)
def fetch_data_and_save_csv(store):
    context = get_current_context()

    store_id = store[0]

    latitude = store[1]
    longitude = store[2]
    start_date = context["params"]["start_date"]
    end_date = context["params"]["end_date"]
    hourly = ",".join(
        [
            "temperature_2m",
            "relativehumidity_2m",
            "apparent_temperature",
            "rain",
            "snowfall",
            "weathercode",
            "windspeed_10m",
            "soil_temperature_0_to_7cm",
        ]
    )

    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly={hourly}&format=csv"

    logging.info(f"Fetching Store {store_id} Data {url}")

    csv_data = requests.get(url, timeout=None).text

    base_dir = "./dags/stage/etl_openmeteo/csv/extracted/"
    filename = f"openmeteo_data_{store_id}.csv"

    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    with open(f"{base_dir}/{filename}", "w") as outfile:
        outfile.write(csv_data)

    return {"filename": filename, "store_id": store_id}
