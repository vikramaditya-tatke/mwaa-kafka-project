from datetime import timedelta

import boto3
import sys
import orjson
import logging

from airflow import DAG
from botocore.exceptions import ClientError
from datetime import datetime
from confluent_kafka import Producer
from tcp_reader import HighPerfTCPReader
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def get_secret(secret_name, region_name="us-east-1"):
    """
    Retrieve secrets from AWS Secrets Manager
    """
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name,
    )
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return orjson.loads(response["SecretString"])
    except ClientError:
        logger.error("Secrets retrival failed")


def create_kafka_producer(config):
    return Producer(config)


def read_logs_from_tcp(host="localhost", port=9999):
    """
    This functions reads JSON logs from a TCP Socket and yields them
    """
    reader = HighPerfTCPReader(host=host, port=port)
    try:
        # Process logs as they arrive
        for log_line in reader:
            # Your processing logic here
            yield log_line
    except KeyboardInterrupt:
        sys.exit()


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivery to {msg.topic()}: {msg.partition()}")


def log_producer(**context):
    """
    Produce log entries into Kafka
    """
    secrets = get_secret("MWAA_Secrets")
    kafka_config = {
        "bootstrap.servers": secrets["KAFKA_BOOTSTRAP_SERVER"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": secrets["KAFKA_SASL_USERNAME"],
        "sasl.password": secrets["KAFKA_SASL_PASSWORD"],
    }

    producer = create_kafka_producer(kafka_config)
    topic = "logs_4"
    print(f"Writing logs to Kafka @ {secrets['KAFKA_BOOTSTRAP_SERVER']}")
    for log_line in read_logs_from_tcp():
        try:
            log_line_bytes = log_line.encode("utf-8")
            producer.produce(topic, log_line_bytes, on_delivery=delivery_report)
            producer.flush()
        except Exception as e:
            logger.error(f"Message delivery failed {e}")
            raise


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": "1",
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="log_generation_pipeline",
    default_args=default_args,
    description="Generates sample logs",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 3, 7),
    catchup=False,
    tags=["logs", "kafka", "dev"],
)

produce_logs = PythonOperator(
    task_id="generate_and_produce_logs",
    python_callable=log_producer,
    dag=dag,
)
