from abc import ABC, abstractmethod
from pyspark.sql.dataframe import DataFrame

class IMessager(ABC):
    @abstractmethod
    def consume_messages(self, topic: str) -> DataFrame:
        """"""

    @abstractmethod
    def send_message(self, topic: str, df: DataFrame) -> None:
        """"""