import os
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, when, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Define the schema
schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True)
])

def create_spark_session():
    # Detect if running inside Docker or on Windows Host
    is_docker = os.path.exists("/.dockerenv")
    kafka_broker = "kafka:29092" if is_docker else "localhost:9092"
    iceberg_uri = "http://restcatalog:8181" if is_docker else "http://localhost:8181"
    s3_endpoint = "http://minio:9000" if is_docker else "http://localhost:9000"

    print(f"[SPARK] Configuration: Kafka={kafka_broker}, Iceberg={iceberg_uri}, S3={s3_endpoint}")

    spark = SparkSession.builder \
        .appName("FinancialFraudStreaming") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
        .config("spark.sql.catalog.lakehouse.uri", iceberg_uri) \
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", s3_endpoint) \
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
        .config("spark.sql.catalog.lakehouse.client.region", "us-east-1") \
        .config("spark.sql.catalog.lakehouse.s3.region", "us-east-1") \
        .config("spark.sql.defaultCatalog", "lakehouse") \
        .config("spark.sql.catalog.demo.uri", iceberg_uri) \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
        .config("spark.hadoop.fs.s3a.region", "us-east-1") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    return spark, kafka_broker

def process_stream(spark, kafka_broker):
    print(f"[SPARK] Reading from Kafka ({kafka_broker})...")
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", "financial-transactions") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # 0. Load Dynamic Config
    import json
    config_path = "/tmp/config.json" if os.path.exists("/.dockerenv") else "data/config.json"
    
    # Define udf to load config (simplified for demo, usually would use Broadcast variable)
    min_large = 8000
    try:
        with open(config_path, 'r') as f:
            cfg = json.load(f)
            min_large = cfg.get("min_large_transfer", 8000)
    except: pass

    # ENHANCED LOGIC: Flag anomalies based on dynamic configuration
    transformed_df = parsed_df \
        .withColumn("processing_time", current_timestamp()) \
        .fillna({'amount': 0.0}) \
        .withColumn("isFraud", col("isFraud").cast("int")) \
        .withColumn("isFlaggedFraud", 
                    when(
                        (col("isFraud") == 1) | 
                        (col("amount") >= min_large) | 
                        ((col("amount") % 1000 == 0) & (col("amount") >= 3000)), 1
                    ).otherwise(0))

    # Create Lakehouse Table
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.fraud_db")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.fraud_db.transactions (
            step INT, type STRING, amount DOUBLE, nameOrig STRING,
            oldbalanceOrg DOUBLE, newbalanceOrig DOUBLE, nameDest STRING,
            oldbalanceDest DOUBLE, newbalanceDest DOUBLE, isFraud INT,
            isFlaggedFraud INT, processing_time TIMESTAMP
        ) USING iceberg PARTITIONED BY (type)
    """)

    # Sink 1: Write all data to Iceberg
    print("[SPARK] Writing all transactions to Iceberg Lakehouse...")
    iceberg_query = transformed_df \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/transactions/") \
        .toTable("lakehouse.fraud_db.transactions")

    # Sink 2: Filter Anomalies (including suspected ones) and push to fraud-alerts
    print("[SPARK] Filtering anomalies (isFraud=1 OR amount > 5000) for AI Agent...")
    alerts_df = transformed_df.filter(col("isFlaggedFraud") == 1) \
        .select(to_json(struct("*")).alias("value"))

    alerts_query = alerts_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("topic", "fraud-alerts") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/alerts/") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    spark, broker = create_spark_session()
    process_stream(spark, broker)
