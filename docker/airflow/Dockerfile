FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY requirements.txt /tmp/af_requirements.txt
RUN pip install --no-cache-dir -r /tmp/af_requirements.txt

# SparkSubmitOperator runs spark-submit locally in this container (client deploy mode) and
# needs the same Delta/S3A jars the Spark cluster has, but at a path that exists in THIS
# image (the DAG's SPARK_CONF/DELTA_JARS point to /opt/airflow/spark_jars/*).
ARG DELTA_VERSION=3.2.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
USER root
RUN mkdir -p /opt/airflow/spark_jars && \
    curl -fsSL -o /opt/airflow/spark_jars/delta-spark_2.12-${DELTA_VERSION}.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar && \
    curl -fsSL -o /opt/airflow/spark_jars/delta-storage-${DELTA_VERSION}.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar && \
    curl -fsSL -o /opt/airflow/spark_jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSL -o /opt/airflow/spark_jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    chown -R airflow:root /opt/airflow/spark_jars
USER airflow
