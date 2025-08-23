from os import getenv
from pyspark.sql import SparkSession, DataFrame
from src.domain.entities.person import Person
from src.domain.interfaces.imessager import IMessager
from src.domain.interfaces.idatabase_repository import IDatabaseRepository
from pyspark.sql.functions import from_json, col
from src.helper.logger import get_logger

persons_topic = getenv("KAFKA_PERSONS_TOPIC", "")
processed_topic = getenv("KAFKA_PROCESSED_TOPIC", "")
keyspace = getenv("SCYLLA_KEYSPACE", "")
table = getenv("SCYLLA_TABLE", "")
logger = get_logger(__name__)

class PersonEventHandler:
    def __init__(self, spark: SparkSession, messager: IMessager, database_repository: IDatabaseRepository):
        self.spark = spark
        self.messager = messager
        self.database_repository = database_repository

    def handle(self):
        try:
            df = self.messager.consume_messages(persons_topic)
            query = df.writeStream \
                .foreachBatch(lambda df, _: self.__process_value(df)) \
                .outputMode("append") \
                .start()
            query.awaitTermination()
        except Exception as e:
            logger.error(f"Error processing messages: {e}")
            return

    def __process_value(self, df: DataFrame):
        try:
            if df.isEmpty():
                logger.warning("No messages to process.")
                return
            
            df_json = df.withColumn("person", from_json(col("value"), Person.get_schema())) \
                    .select("person.*") \
                    .withColumn("age_in_months", col("age") * 12)
            
            self.database_repository.insert_person(df_json, keyspace, table)
            self.messager.send_message(processed_topic, df_json)
        except Exception as e:
            logger.error(f"Error processing value: {e}")