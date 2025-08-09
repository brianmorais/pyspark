from pyspark.sql.types import StructType, StringType, IntegerType

class Person:
    @staticmethod
    def get_schema():
        return StructType() \
            .add("name", StringType()) \
            .add("age", IntegerType()) \
            .add("city", StringType()) \
            .add("age_in_months", IntegerType())
    