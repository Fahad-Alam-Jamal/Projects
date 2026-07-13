##  cd ~/Fahad/Kafka/App_Traffic_Monitor

##  source /home/victus/Python-3/bin/activate

##  python Event_Logger.py



from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import atexit



def stop_logger():
    if query.isActive:
        query.stop()
    spark.stop()
    print("Logger Stopped")


def process_batch(batch_df, batch_id):

    delta_table = DeltaTable.forPath(spark, delta_path)

    delete_ids_df = (
        batch_df
        .filter((col('event')=='account_deleted') | (col('event')=='session_ended'))
        .select("user_id")
        .distinct()
    )

    if delete_ids_df.take(1):

        delta_table.alias("t").merge(
            delete_ids_df.alias("d"),
            "t.user_id = d.user_id"
        ).whenMatchedDelete().execute()


    cleaned_batch_df = batch_df.filter((col('event')!='account_deleted') & (col('event')!='session_ended'))

    if cleaned_batch_df.take(1):
        cleaned_batch_df.write.format("delta").mode("append").save(delta_path)


# Kafka configs
KAFKA_TOPIC = "app-events"
KAFKA_SERVER = "localhost:9092"
delta_path = "/home/victus/Fahad/Kafka/App_Traffic_Monitor/App_Traffic_Delta"

spark = (
    SparkSession.builder
    .appName("Event_Logger")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


event_schema = ArrayType(
    StructType()
        .add("user_id", IntegerType())
        .add("state", StringType())
        .add("event_id", IntegerType())
        .add("device_id", IntegerType())
)

# Read stream from Kafka
raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Kafka value is bytes → string
string_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

# Parse JSON array
parsed_df = string_df.select(from_json(col("json_str"), event_schema).alias("events"))

events_df = parsed_df.select(explode(col("events")).alias("event"))

df = events_df.select(
    col("event.user_id"),
    col("event.state"),
    col("event.event_id"),
    col("event.device_id")
)


# PREPROCESSING
df = df.withColumn("state", regexp_replace(col("state"), " ", ""))
df = df.withColumn(
    'state',
    when(col('state') == 'AndamanandNicobarIslands', 'AndamanandNicobar')
    .otherwise(col('state'))
)

# Events lookup
events_lookup = spark.read.parquet('/home/victus/Fahad/Kafka/App_Traffic_Monitor/Look_UP_Data/Events.parquet')
# Devices lookup
device_lookup = spark.read.parquet('/home/victus/Fahad/Kafka/App_Traffic_Monitor/Look_UP_Data/Devices.parquet')

df = df.join(events_lookup, df.event_id == events_lookup.Event_ID, "left").drop("Event_ID")
df = df.join(device_lookup, df.device_id == device_lookup.Device_ID, "left").drop("Device_ID")

ready_df = df.select("user_id", "state", "event", "device")


# Write stream to local Delta Lake
query = (
    ready_df.writeStream
    .foreachBatch(process_batch)
    .option("path", delta_path)
    .option("checkpointLocation", "/home/victus/Fahad/Kafka/App_Traffic_Monitor/metadata/app_events_checkpoint")
    .start()
)

atexit.register(stop_logger)

query.awaitTermination()

