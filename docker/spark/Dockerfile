# Bitnami's free bitnami/spark images were pulled behind a paywall (Bitnami Secure Images) -
# bitnami/spark:3.5 no longer resolves on Docker Hub. Using the official Apache Spark image
# instead (same one already proven working in the upi-fraud-detection-pipeline project).
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

ARG DELTA_VERSION=3.2.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

RUN curl -fsSL -o /opt/spark/jars/delta-spark_2.12-${DELTA_VERSION}.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar && \
    curl -fsSL -o /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar && \
    curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Install the Python-side delta-spark package (the JAR alone only gives Scala/Java bindings;
# `from delta.tables import DeltaTable` needs this). --no-deps avoids pip pulling in a second
# pyspark install that could conflict with the image's bundled Spark.
RUN pip install --no-cache-dir --no-deps delta-spark==${DELTA_VERSION}

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
