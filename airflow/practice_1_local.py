import pendulum
import os
from airflow.decorators import dag, task

home = os.environ["HOME"]
cwd = f"{home}/workspace/macdao/de101"

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def practice_1_local():
    @task.bash(cwd = cwd)
    def load() -> str:
        return "source .venv/bin/activate; spark-submit spark/load.py local data_lake/1"

    @task.bash(cwd = cwd)
    def processing() -> str:
        return "source .venv/bin/activate; spark-submit spark/processing_daily_summary.py data_lake/1 data_lake/2_daily_summary"
    
    @task.bash(cwd = cwd)
    def publish() -> str:
        return "source .venv/bin/activate; spark-submit spark/publish.py data_lake/2_daily_summary data_lake/3_daily_summary"

    load()
    processing()
    publish()

practice_1_local()