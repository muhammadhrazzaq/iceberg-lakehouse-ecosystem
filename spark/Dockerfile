# spark/Dockerfile
FROM bitnamilegacy/spark:3.5.3

USER root

RUN pip install --no-cache-dir \
    requests==2.31.0 \
    python-dotenv==1.0.1 \
    fastavro==1.9.4

WORKDIR /opt/spark-apps

COPY streaming_job.py .

CMD ["/opt/bitnami/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--packages", \
     "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-avro_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
     "/opt/spark-apps/streaming_job.py"]