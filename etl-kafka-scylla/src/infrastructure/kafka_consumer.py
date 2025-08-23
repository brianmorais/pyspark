from os import getenv
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from src.domain.interfaces.imessager import IMessager
from pyspark.sql.functions import to_json, struct, col

bootstrap_servers = getenv("KAFKA_BOOTSTRAP_SERVERS", "")

class KafkaConsumer(IMessager):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def consume_messages(self, topic: str) -> DataFrame:
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .load()
        
        return df.selectExpr("CAST(value AS STRING)")

    def send_message(self, topic: str, df: DataFrame) -> None:
        df_to_kafka = df.withColumn(
            "value",
            to_json(struct([col(c) for c in df.columns]))
        ).selectExpr("CAST(value AS BINARY)")

        df_to_kafka.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", topic) \
            .save()