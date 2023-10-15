from airflow.decorators import task
import logging


@task(task_id="start")
def start():
    logging.info("Start")
