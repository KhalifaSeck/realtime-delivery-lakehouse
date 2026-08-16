FROM eclipse-temurin:11-jdk AS java

FROM python:3.10-slim

WORKDIR /app

COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

COPY streaming/requirements.txt /app/streaming/requirements.txt
RUN pip install --no-cache-dir -r /app/streaming/requirements.txt

# SPARK_HOME pour que spark-submit trouve les JARs
ENV SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark

# Tous les JARs (Kafka + Azure) téléchargés au build
RUN curl -sL "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.4/spark-sql-kafka-0-10_2.12-3.3.4.jar" -o "$SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.3.4.jar" && \
    curl -sL "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.4/spark-token-provider-kafka-0-10_2.12-3.3.4.jar" -o "$SPARK_HOME/jars/spark-token-provider-kafka-0-10_2.12-3.3.4.jar" && \
    curl -sL "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar" -o "$SPARK_HOME/jars/kafka-clients-2.8.1.jar" && \
    curl -sL "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" -o "$SPARK_HOME/jars/commons-pool2-2.11.1.jar" && \
    curl -sL "https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar" -o "$SPARK_HOME/jars/lz4-java-1.8.0.jar" && \
    curl -sL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.2/hadoop-azure-3.3.2.jar" -o "$SPARK_HOME/jars/hadoop-azure-3.3.2.jar" && \
    curl -sL "https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar" -o "$SPARK_HOME/jars/azure-storage-8.6.6.jar" && \
    curl -sL "https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar" -o "$SPARK_HOME/jars/wildfly-openssl-1.0.7.Final.jar"

COPY streaming/ /app/streaming/

CMD ["python", "-m", "streaming.main"]