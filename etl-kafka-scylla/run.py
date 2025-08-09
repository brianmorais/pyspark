from os import environ

environ["KAFKA_PERSONS_TOPIC"] = "persons"
environ["KAFKA_PROCESSED_TOPIC"] = "processed"
environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
environ["SCYLLA_KEYSPACE"] = "etl"
environ["SCYLLA_TABLE"] = "persons"
environ["SCYLLA_HOST"] = "localhost"
environ["SCYLLA_PORT"] = "9042"

from src.application.main import main

if __name__ == "__main__":
    main()