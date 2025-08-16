FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y openjdk-21-jdk \
    && apt-get install -y wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark

RUN wget https://dlcdn.apache.org/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz \
    && tar -xzf spark-4.0.0-bin-hadoop3.tgz --strip-components=1 \
    && rm spark-4.0.0-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

WORKDIR /workspace
