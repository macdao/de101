# Local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
spark-submit src/1-load/local.py
# Practice 1
spark-submit src/2-processing/processing_daily_summary.py data_lake/1 data_lake/2_daily_summary
spark-submit src/3-publish/publish.py data_lake/2_daily_summary data_lake/3_daily_summary
# Practice 2
spark-submit src/2-processing/processing_yearly_summary.py data_lake/1 data_lake/2_yearly_summary
spark-submit src/3-publish/publish.py data_lake/2_yearly_summary data_lake/3_yearly_summary
```

## run in AirFlow

```bash
# Practice 1
cp airflow/practice_1_local.py ~/airflow/dags/
# Practice 2
cp airflow/practice_2_local.py ~/airflow/dags/
```

# EMR

Create cluster with bootstrap action: `bootstrap.sh`

1. Load - `$S3_BUCKET/script/emr.py`
2. Processing (Practice 1) - `spark-submit --deploy-mode cluster $S3_BUCKET/script/processing_daily_summary.py $S3_BUCKET/output/1 $S3_BUCKET/output/2_daily_summary`
3. Processing (Practice 2) - `spark-submit --deploy-mode cluster $S3_BUCKET/script/processing_yearly_summary.py $S3_BUCKET/output/1 $S3_BUCKET/output/2_yearly_summary`
4. Publish (Practice 1) - `spark-submit --deploy-mode cluster $S3_BUCKET/script/publish.py $S3_BUCKET/output/2_daily_summary $S3_BUCKET/output/3_daily_summary`
5. Publish (Practice 2) - `spark-submit --deploy-mode cluster $S3_BUCKET/script/publish.py $S3_BUCKET/output/2_yearly_summary $S3_BUCKET/output/3_yearly_summary`