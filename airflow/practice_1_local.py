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
        return ".venv/bin/spark-submit src/1-load/local.py"

    @task.bash(cwd = cwd)
    def processing() -> str:
        return ".venv/bin/spark-submit src/2-processing/daily_summary_local.py"
    
    @task.bash(cwd = cwd)
    def publish() -> str:
        return ".venv/bin/spark-submit src/3-publish/daily_summary_local.py"

    load()
    processing()
    publish()

practice_1_local()