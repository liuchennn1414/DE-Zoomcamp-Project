FROM apache/airflow:2.10.5

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
        default-jdk \
        wget \
        curl \
        gnupg && \
    apt-get clean

# Set JAVA_HOME environment variable
RUN update-java-alternatives -l && \
    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "export JAVA_HOME=$JAVA_HOME" >> /etc/environment

ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Google Cloud SDK
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && \
    apt-get install -y google-cloud-sdk

# Install Spark
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install Spark connectors
RUN mkdir -p $SPARK_HOME/jars && \
    wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar -P $SPARK_HOME/jars/ && \
    wget https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest_2.12.jar -P $SPARK_HOME/jars/

# Copy Hadoop configuration
COPY hadoop-conf /opt/hadoop-conf
ENV HADOOP_CONF_DIR=/opt/hadoop-conf

USER airflow

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory to Airflow
WORKDIR /opt/airflow

# Copy DAGs and Spark transformation scripts
COPY dags /opt/airflow/dags
COPY --chown=airflow:airflow spark_transformation/transformation.py /opt/airflow/spark_transformation/transformation.py

# Ensure the script is executable
RUN chmod +x /opt/airflow/spark_transformation/transformation.py


