@echo off
echo ======================================================
echo  Spark in Docker Runner (Bypass Windows dependencies)
echo ======================================================
echo 1. Copying streaming_job.py and config.json to Spark Container...
docker cp %~dp0..\src\pipeline\streaming_job.py spark-iceberg:/tmp/streaming_job.py
docker cp %~dp0..\data\config.json spark-iceberg:/tmp/config.json

echo 2. Submitting Spark Job inside the container...
docker exec -e AWS_REGION=us-east-1 -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=password spark-iceberg spark-submit ^
  --conf spark.driver.extraJavaOptions="-Daws.region=us-east-1" ^
  --conf spark.executor.extraJavaOptions="-Daws.region=us-east-1" ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 ^
  /tmp/streaming_job.py
