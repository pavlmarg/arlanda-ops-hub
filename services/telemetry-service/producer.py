import time
import uuid
import json
import logging
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ---------------------------------------------------------
# JSON Logger Configuration
# ---------------------------------------------------------
class JSONFormatter(logging.Formatter):
    """Custom formatter to output logs as structured JSON."""
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "service": "telemetry-producer",
            "message": record.getMessage()
        }
        # Include exception details if an error occurs
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

# Initialize the logger
logger = logging.getLogger("TelemetryProducer")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(JSONFormatter())
logger.addHandler(console_handler)

# ---------------------------------------------------------
# Application Configuration
# ---------------------------------------------------------
KAFKA_BROKER = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC_NAME = 'arn.flight.status.updated'
SCHEMA_PATH = '../../schemas/avro/Flight_Status_Update.avsc'

def load_schema(schema_path):
    """Reads the Avro schema from the central schemas folder."""
    try:
        with open(schema_path, 'r') as file:
            return file.read()
    except FileNotFoundError as e:
        logger.error(f"Schema file not found at {schema_path}")
        raise e

def delivery_report(err, msg):
    """Callback triggered by Kafka to confirm if the message was delivered."""
    if err is not None:
        logger.error(f"Delivery failed for record {msg.key()}: {err}")
    else:
        logger.info(f"Successfully produced record to topic '{msg.topic()}' (partition {msg.partition()})")

def main():
    logger.info("Initializing Telemetry Producer service...")

    # Initialize Schema Registry Client and Avro Serializer
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    avro_schema_str = load_schema(SCHEMA_PATH)
    avro_serializer = AvroSerializer(
        schema_registry_client,
        avro_schema_str
    )

    # Initialize Kafka Producer
    producer_conf = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(producer_conf)
    logger.info("Successfully connected to Kafka broker.")

    # Construct the Dummy Event
    correlation_id = str(uuid.uuid4())
    current_time_ms = int(time.time() * 1000)

    flight_event = {
        "id": str(uuid.uuid4()),
        "source": "urn:arlanda:telemetry-service",
        "specversion": "1.0",
        "type": "Flight_Status_Update",
        "time": current_time_ms,
        "correlation_id": correlation_id,
        "data": {
            "flight_number": "SK400",
            "status": "DELAYED",
            "estimated_arrival": current_time_ms + 3600000
        }
    }

    logger.info(f"Preparing to send event for {flight_event['data']['flight_number']}...", extra={"flight_number": flight_event['data']['flight_number']})

    # Serialize and Produce to Kafka
    producer.produce(
        topic=TOPIC_NAME,
        key=flight_event['data']['flight_number'],
        value=avro_serializer(flight_event, SerializationContext(TOPIC_NAME, MessageField.VALUE)),
        on_delivery=delivery_report
    )

    # Flush ensures that the message is actually sent out to the broker before the script exits
    producer.flush()
    logger.info("Producer flush complete. Exiting application.")

if __name__ == '__main__':
    main()