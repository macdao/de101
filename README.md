## run local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
spark-submit src/1-load/local.py
spark-submit src/2-processing/local.py
spark-submit src/3-publish/local.py
```

## run in AirFlow

```bash
cp airflow/practice_1_local.py ~/airflow/dags/
```