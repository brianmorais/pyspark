from abc import ABC, abstractmethod
from pyspark.sql.dataframe import DataFrame

class IDatabaseRepository(ABC):
    @abstractmethod
    def insert_person(self, df: DataFrame, keyspace: str, table: str):
        """"""