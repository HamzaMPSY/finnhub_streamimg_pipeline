import os
import uuid

import avro.io
import avro.schema
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0 pyspark-shell'


makeUUID = udf(lambda: str(uuid.uuid1()), StringType())


class TradeProcessor:

    def __init__(self):
        self.app_name = "TradeProcessor"
        self.master_host = os.getenv(
            'SPARK_MASTER') + ':' + os.getenv('SPARK_PORT')
        self.kafka_bootstrap_server = os.getenv(
            'KAFKA_SERVER') + ':' + os.getenv('KAFKA_PORT')
        self.kafka_topic = os.getenv('KAFKA_TOPIC_NAME')
        self.trade_schema = open('trades.avsc', "r").read()
        self.spark = SparkSession.builder.appName(self.app_name).master(self.master_host
                                                                        ).getOrCreate()

    def process(self):
        inputDF = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_server) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        # explode the data from Avro

        expandedDF = inputDF.withColumn("avroData", from_avro("value", self.trade_schema))\
            .select("avroData.*")\
            .select(explode("data"), "type")\
            .select("col.*")
        # rename columns and add proper timestamps
        finalDF = expandedDF \
            .withColumn("uuid", lit(uuid.uuid1().hex))\
            .withColumnRenamed("c", "trade_conditions")\
            .withColumnRenamed("p", "price")\
            .withColumnRenamed("s", "symbol")\
            .withColumnRenamed("t", "trade_timestamp")\
            .withColumnRenamed("v", "volume")\
            .withColumn("trade_timestamp", (col("trade_timestamp") / 1000).cast("timestamp"))\
            .withColumn("ingest_timestamp", current_timestamp())

        finalDF.writeStream \
            .format("console") \
            .outputMode("append") \
            .start() \
            .awaitTermination()


if __name__ == "__main__":
    processor = TradeProcessor()
    processor.process()
