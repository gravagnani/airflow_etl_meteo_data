from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import pandas as pd
import logging


@task(task_id="merge_into_target", weight_rule="upstream")
def merge_into_target(data):
    transform_table = data["transform_table"]
    store_id = data["store_id"]

    logging.info(f"Merging {transform_table} table")

    # Get Postgres connection
    postgres_hook = PostgresHook(postgres_conn_id="postgres", schema="airflow")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    # Create target table if not exists
    cur.execute(
        query="""
            CREATE TABLE IF NOT EXISTS data.openmeteo_data (
                location_id varchar NULL,
                latitude varchar NULL,
                longitude varchar NULL,
                utc_offset_seconds numeric NULL,
                timezone varchar NULL,
                elevation varchar NULL,
                "time" timestamptz NULL,
                temperature_2m numeric NULL, 
                apparent_temperature numeric NULL,
                relativehumidity_2m numeric NULL,
                rain numeric NULL,
                snowfall numeric NULL,
                weathercode numeric NULL,
                windspeed_10m numeric NULL,
                soil_temperature_0_to_7cm numeric NULL,
                created_at  timestamptz NULL,
                updated_at  timestamptz NULL,
                PRIMARY KEY (location_id, "time")
            );
        """
    )

    # Merge data
    cur.execute(
        query=f"""
            INSERT INTO data.openmeteo_data
            SELECT 
                s.location_id,
                s.latitude, 
                s.longitude, 
                s.utc_offset_seconds, 
                s.timezone, 
                s.elevation, 
                s."time",
                s.temperature_2m,
                s.apparent_temperature,
                s.relativehumidity_2m,
                s.rain,
                s.snowfall,
                s.weathercode,
                s.windspeed_10m,
                s.soil_temperature_0_to_7cm,
                now() created_at,
                now() updated_at
            FROM staging.{transform_table} AS s
            ON CONFLICT (
                location_id,
                "time"
            ) DO UPDATE SET
                latitude = EXCLUDED.latitude, 
                longitude = EXCLUDED.longitude, 
                utc_offset_seconds = EXCLUDED.utc_offset_seconds, 
                timezone = EXCLUDED.timezone, 
                elevation = EXCLUDED.elevation, 
                temperature_2m = EXCLUDED.temperature_2m,
                apparent_temperature = EXCLUDED.apparent_temperature,
                relativehumidity_2m = EXCLUDED.relativehumidity_2m,
                rain = EXCLUDED.rain,
                snowfall = EXCLUDED.snowfall,
                weathercode = EXCLUDED.weathercode,
                windspeed_10m = EXCLUDED.windspeed_10m,
                soil_temperature_0_to_7cm = EXCLUDED.soil_temperature_0_to_7cm,
                updated_at = now()
            ;
        """
    )
    conn.commit()
