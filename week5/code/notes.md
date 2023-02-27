export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

URL="spark://instance-2.us-west4-b.c.dataeng-375609.internal:7077"

spark-submit \
    --master "${URL}" \
    06_spark_sql.py \
    --input_green=data/pq/green/2021/*/ \
    --input_yellow=data/pq/yellow/2021/*/ \
    --output=data/report-2021

URL="spark://de-zoomcamp.europe-west1-b.c.de-zoomcamp-nytaxi.internal:7077"

spark-submit \
    --master="${URL}" \
    06_spark_sql.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report-2021


--input_green=gs://new_york_city_trips_dataset/pq/green/2021/*/ \
--input_yellow=gs://new_york_city_trips_dataset/pq/yellow/2021/*/ \
--output=gs://new_york_city_trips_dataset/report-2021