from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import pandas as pd
import logging


@task(task_id="transform_staging_data", weight_rule="upstream")
def transform_staging_data(data):
    extract_table = data["extract_table"]
    store_id = data["store_id"]

    logging.info(f"Transforming extract table {extract_table}")

    # Estract from staging table
    postgres_hook = PostgresHook(postgres_conn_id="postgres", schema="airflow")
    df = postgres_hook.get_pandas_df(
        sql=f"""
            SELECT latitude, 
                   longitude, 
                   elevation, 
                   utc_offset_seconds, 
                   timezone, 
                   timezone_abbreviation, 
                   "time", 
                   "temperature_2m (°C)", 
                   "apparent_temperature (°C)",
                   "relativehumidity_2m (%%)",
                   "rain (mm)",
                   "snowfall (cm)",
                   "weathercode (wmo code)",
                   "windspeed_10m (km/h)",
                   "soil_temperature_0_to_7cm (°C)"
            FROM staging.{extract_table}
        """,
        parameters={},
    )

    # Add column
    df["location_id"] = store_id

    # Rename columns
    df.rename(columns={"temperature_2m (°C)": "temperature_2m"}, inplace=True)
    df.rename(
        columns={"apparent_temperature (°C)": "apparent_temperature"}, inplace=True
    )
    df.rename(columns={"relativehumidity_2m (%)": "relativehumidity_2m"}, inplace=True)
    df.rename(
        columns={"rain (mm)": "rain"},
        inplace=True,
    )
    df.rename(columns={"snowfall (cm)": "snowfall"}, inplace=True)
    df.rename(columns={"weathercode (wmo code)": "weathercode"}, inplace=True)
    df.rename(columns={"windspeed_10m (km/h)": "windspeed_10m"}, inplace=True)
    df.rename(
        columns={"soil_temperature_0_to_7cm (°C)": "soil_temperature_0_to_7cm"},
        inplace=True,
    )

    # Change Data Types
    df["elevation"] = pd.to_numeric(df["elevation"])
    df["utc_offset_seconds"] = pd.to_numeric(df["utc_offset_seconds"])
    df["temperature_2m"] = pd.to_numeric(df["temperature_2m"])
    df["apparent_temperature"] = pd.to_numeric(df["apparent_temperature"])
    df["relativehumidity_2m"] = pd.to_numeric(df["relativehumidity_2m"])
    df["rain"] = pd.to_numeric(df["rain"])
    df["snowfall"] = pd.to_numeric(df["snowfall"])
    df["weathercode"] = pd.to_numeric(df["weathercode"])
    df["weathercode"] = pd.to_numeric(df["weathercode"])
    df["windspeed_10m"] = pd.to_numeric(df["windspeed_10m"])
    df["soil_temperature_0_to_7cm"] = pd.to_numeric(df["soil_temperature_0_to_7cm"])

    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M", utc=True)
    # df["time"] = df.apply(lambda x: x["time"].tz_convert("Europe/Berlin"), axis=1)

    # Load into staging table
    transform_table = f"t_openmeteo_data_{store_id}"
    df.to_sql(
        name=transform_table,
        con=postgres_hook.get_sqlalchemy_engine(),
        schema="staging",
        index=False,
        if_exists="replace",
        chunksize=1000,
    )

    return {"transform_table": transform_table, "store_id": store_id}
