import json
import time
from datetime import datetime, timezone
import logging
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaBigQueryConsumer:
    def __init__(self,
                 kafka_servers: str,
                 topic: str,
                 project_id: str,
                 bq_dataset: str,
                 bq_table: str,
                 batch_size: int = 100,
                 timeout_seconds: int = 300,
                 use_gcp_auth: bool = False):
        self.kafka_servers = kafka_servers
        self.topic = topic
        self.project_id = project_id
        self.bq_dataset = bq_dataset
        self.bq_table = bq_table
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.use_gcp_auth = use_gcp_auth
        
        # Initialize BigQuery client
        self.bq_client = bigquery.Client(project=project_id)
        self.table_id = f"{project_id}.{bq_dataset}.{bq_table}"
        
        # Create BigQuery table if it doesn't exist
        self._ensure_table_exists()
        
        # Initialize Kafka consumer
        self.consumer = self._create_consumer()
    
    def _ensure_table_exists(self):
        """Create BigQuery table if it doesn't exist."""
        schema = [
            bigquery.SchemaField("sepal_length", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("sepal_width", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("petal_length", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("petal_width", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("sample_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("ingestion_time", "TIMESTAMP", mode="REQUIRED")
        ]
        
        try:
            table = self.bq_client.get_table(self.table_id)
            logger.info(f"Table {self.table_id} already exists")
        except NotFound:
            table = bigquery.Table(self.table_id, schema=schema)
            table = self.bq_client.create_table(table)
            logger.info(f"Created table {self.table_id}")
    
    def _create_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer with appropriate configuration."""
        if self.use_gcp_auth:
            return self._create_gcp_consumer()
        else:
            return self._create_local_consumer()
    
    def _create_local_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer for local development."""
        logger.info("Using local Kafka consumer configuration")
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            consumer_timeout_ms=self.timeout_seconds * 1000,
            group_id=f"bq-consumer-{int(time.time())}",
            security_protocol='PLAINTEXT'
        )
    
    def _create_gcp_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer for GCP Managed Kafka."""
        import base64
        import os
        from pathlib import Path
        
        logger.info("Using GCP Managed Kafka consumer configuration")
        
        # Path to service account key
        key_path = Path(__file__).parent.parent.parent.parent / "gcp-key.json"
        
        if key_path.exists():
            with open(key_path, 'r') as f:
                key_data = f.read()
            sasl_password = base64.b64encode(key_data.encode()).decode()
            sasl_username = "kfp-mlops@deeplearning-sahil.iam.gserviceaccount.com"
        else:
            raise FileNotFoundError(f"Service account key not found at {key_path}")
        
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            consumer_timeout_ms=self.timeout_seconds * 1000,
            group_id=f"bq-consumer-{int(time.time())}",
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=sasl_username,
            sasl_plain_password=sasl_password,
            ssl_check_hostname=False,
            ssl_cafile=None
        )
    
    def consume_and_load(self):
        """Consume messages from Kafka and load them to BigQuery."""
        logger.info(f"Starting Kafka consumer for topic: {self.topic}")
        
        consumed_data = []
        start_time = time.time()
        total_records = 0
        
        try:
            for message in self.consumer:
                if message.value:
                    # Add ingestion timestamp
                    record = message.value
                    record['ingestion_time'] = datetime.now(timezone.utc).isoformat()
                    consumed_data.append(record)
                    
                    logger.info(f"Consumed message: {record['sample_id']}")
                    
                    # Process batch when reached batch_size
                    if len(consumed_data) >= self.batch_size:
                        self._load_batch_to_bigquery(consumed_data)
                        total_records += len(consumed_data)
                        consumed_data = []
                    
                    # Check timeout
                    if time.time() - start_time > self.timeout_seconds:
                        logger.info(f"Timeout reached after {self.timeout_seconds} seconds")
                        break
        
        except Exception as e:
            logger.error(f"Error in Kafka consumer: {e}")
            raise
        
        finally:
            # Process remaining data
            if consumed_data:
                self._load_batch_to_bigquery(consumed_data)
                total_records += len(consumed_data)
            
            self.consumer.close()
            logger.info("Kafka consumer closed")
            logger.info(f"Total records processed: {total_records}")
            
        return total_records
    
    def _load_batch_to_bigquery(self, batch_data):
        """Load a batch of data to BigQuery."""
        df = pd.DataFrame(batch_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['ingestion_time'] = pd.to_datetime(df['ingestion_time'])
        
        # Define schema for loading
        schema = [
            bigquery.SchemaField("sepal_length", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("sepal_width", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("petal_length", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("petal_width", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("sample_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("ingestion_time", "TIMESTAMP", mode="REQUIRED")
        ]
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=schema
        )
        
        job = self.bq_client.load_table_from_dataframe(
            df, self.table_id, job_config=job_config
        )
        job.result()
        
        logger.info(f"Loaded {len(batch_data)} records to BigQuery table {self.table_id}")
    
    def close(self):
        """Close the Kafka consumer."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Consume Kafka messages and load to BigQuery')
    parser.add_argument('--kafka-servers', required=True,
                       help='Kafka bootstrap servers (e.g., localhost:9092)')
    parser.add_argument('--topic', default='iris-inference-data',
                       help='Kafka topic name')
    parser.add_argument('--project-id', required=True,
                       help='GCP Project ID')
    parser.add_argument('--bq-dataset', required=True,
                       help='BigQuery dataset name')
    parser.add_argument('--bq-table', required=True,
                       help='BigQuery table name')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size for BigQuery loading')
    parser.add_argument('--timeout', type=int, default=300,
                       help='Timeout in seconds')
    parser.add_argument('--use-gcp-auth', action='store_true',
                       help='Use GCP authentication for managed Kafka (default: local)')
    
    args = parser.parse_args()
    
    consumer = KafkaBigQueryConsumer(
        kafka_servers=args.kafka_servers,
        topic=args.topic,
        project_id=args.project_id,
        bq_dataset=args.bq_dataset,
        bq_table=args.bq_table,
        batch_size=args.batch_size,
        timeout_seconds=args.timeout,
        use_gcp_auth=args.use_gcp_auth
    )
    
    try:
        consumer.consume_and_load()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()