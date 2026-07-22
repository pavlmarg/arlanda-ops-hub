import os
import time
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

def wait_for_registry(max_attempts=15, delay_seconds=2):
    """Blocks until the Schema Registry is reachable, or gives up."""
    for attempt in range(1, max_attempts + 1):
        try:
            sr_client.get_subjects()
            print(f"Schema Registry at {SCHEMA_REGISTRY_URL} is reachable.")
            return
        except Exception as e:
            print(f"[{attempt}/{max_attempts}] Schema Registry not ready yet ({e}). Retrying in {delay_seconds}s...")
            time.sleep(delay_seconds)
    raise RuntimeError(f"Schema Registry at {SCHEMA_REGISTRY_URL} never became reachable.")


def register_avro_schema(schema_path, subject_name):
    # Read the local .avsc file
    with open(schema_path, 'r') as file:
        schema_str = file.read()

    # Create a Schema object
    avro_schema = Schema(schema_str, schema_type="AVRO")

    # Register the schema with the Confluent Schema Registry.
    try:
        schema_id = sr_client.register_schema(subject_name=subject_name, schema=avro_schema)
        print(f"Registered '{subject_name}' -> schema id {schema_id}")
        return schema_id
    except SchemaRegistryError as e:
        print(f"Failed to register schema for subject '{subject_name}': {e}")
        raise


if __name__ == "__main__":
    wait_for_registry()

    # Register the Flight Status schema
    register_avro_schema(
        schema_path="schemas/avro/Flight_Status_Update.avsc",
        subject_name="arn.flight.status.updated-value"
    )

    # Register the Weather Alert schema
    register_avro_schema(
        schema_path="schemas/avro/Weather_Alert.avsc",
        subject_name="arn.weather.alert.issued-value"
    )

    # Register the Baggage Scanned schema 
    register_avro_schema(
        schema_path="schemas/avro/Baggage_Scanned.avsc",
        subject_name="arn.baggage.scanned-value"
    )

    # Register the Baggage Routing Command schema 
    register_avro_schema(
        schema_path="schemas/avro/Baggage_Routing_Command.avsc",
        subject_name="arn.baggage.routing_command-value"
    )

    # Register the Baggage Hub Status Changed schema 
    register_avro_schema(
        schema_path="schemas/avro/Baggage_Hub_Status_Changed.avsc",
        subject_name="arn.baggage.hub.status_changed-value"
    )

    print("All schemas registered successfully.")