from airflow.decorators import task
import logging


@task(task_id="end")
def end():
    logging.info("End")
