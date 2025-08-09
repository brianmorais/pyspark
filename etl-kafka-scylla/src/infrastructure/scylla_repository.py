from pyspark.sql.dataframe import DataFrame
from src.domain.interfaces.iscylla_repository import IScyllaRepository
from src.infrastructure.scylla_connection import ScyllaConnection

class ScyllaRepository(IScyllaRepository):
    def __init__(self, scylla_connection: ScyllaConnection):
        self.scylla_connection = scylla_connection

    def insert_person(self, df: DataFrame, keyspace: str, table: str):
        self.scylla_connection.insert(df, keyspace, table)
