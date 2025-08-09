from pyspark.sql.dataframe import DataFrame

class ScyllaConnection:
    def insert(self, df: DataFrame, keyspace: str, table: str):
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(keyspace=keyspace, table=table) \
            .mode("append") \
            .save()