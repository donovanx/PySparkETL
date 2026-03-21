# Use a base with Java + Python (we'll install Spark ourselves)
FROM ubuntu:22.04

# Install Java, wget, Python, pip
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install Spark manually (choose a stable version, e.g., 3.5.1)
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3

RUN wget -O /tmp/spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    tar xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# Add Spark to PATH
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Install PySpark + other Python deps
RUN pip3 install --no-cache-dir pyspark pandas pyarrow

# Download PostgreSQL JDBC driver into Spark's jars
RUN wget -O ${SPARK_HOME}/jars/postgresql-42.7.1.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

ENV WORKDIR=/app

WORKDIR $WORKDIR

# Copy your code
COPY src/ ${WORKDIR}/src/
COPY configs/ ${WORKDIR}/configs/

# Default command: run your main job
CMD ["python3", "src/main.py"]
