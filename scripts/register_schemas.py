from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

# Initialize the Schema Registry Client
sr_client = SchemaRegistryClient({'url': 'http://localhost:8081'})

def register_avro_schema(schema_path, subject_name):
    # Read the local .avsc file
    with open(schema_path, 'r') as file:
        schema_str = file.read()
        
    # Create a Schema object
    avro_schema = Schema(schema_str, schema_type="AVRO")
    
    # Register the schema with the Confluent Schema Registry
    schema_id = sr_client.register_schema(subject_name=subject_name, schema=avro_schema)
    
    return schema_id

if __name__ == "__main__":
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