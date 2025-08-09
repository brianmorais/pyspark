from os import environ
from pyspark.sql import SparkSession
from src.handler.person_event_handler import PersonEventHandler
from src.infrastructure.kafka_consumer import KafkaConsumer
from src.infrastructure.scylla_connection import ScyllaConnection
from src.infrastructure.scylla_repository import ScyllaRepository
from src.helper.logger import get_logger

scylla_host = environ.get("SCYLLA_HOST", "")
scylla_port = environ.get("SCYLLA_PORT", "9042")
logger = get_logger(__name__)

def main():
    try:
        spark = SparkSession.builder \
            .appName("KafkaToScyllaETL") \
            .master("local[*]") \
            .config("spark.jars.packages", ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
                "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1"
            ])) \
            .config("spark.cassandra.connection.host", scylla_host) \
            .config("spark.cassandra.connection.port", scylla_port) \
            .getOrCreate()
        
        kafka_consumer = KafkaConsumer(spark)
        scylla_connection = ScyllaConnection()
        scylla_repository = ScyllaRepository(scylla_connection)
        handler = PersonEventHandler(spark, kafka_consumer, scylla_repository)
        handler.handle()
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
