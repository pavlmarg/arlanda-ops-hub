import logging
import json
import os
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

# Import your custom mathematical routing engine
from routing_engine import BaggageRoutingEngine

# Setup standard Logger for the service
logger = logging.getLogger("baggage-routing")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)

class BaggageRoutingService:
    def __init__(self):
        logger.info("Booting Baggage Routing Microservice...")
        
        #  Initialize the Core Routing Engine 
        self.engine = BaggageRoutingEngine()
        
        # Infrastructure Config 
        self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        self.schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
        
        # Define the Event Topics
        self.consume_topic = "arn.baggage.scanned"
        self.produce_topic = "arn.baggage.routing_command"
        
        # Setup Schema Registry Client
        self.sr_client = SchemaRegistryClient({'url': self.schema_registry_url})
        
        # Setup Deserializer for incoming Avro events
        bag_scanned_schema = self.sr_client.get_latest_version(self.consume_topic + "-value").schema
        self.avro_deserializer = AvroDeserializer(self.sr_client, bag_scanned_schema.schema_str)
        
        # Setup Serializer for outgoing Avro commands
        routing_command_schema = self.sr_client.get_latest_version(self.produce_topic + "-value").schema
        self.avro_serializer = AvroSerializer(self.sr_client, routing_command_schema.schema_str)
        self.string_serializer = StringSerializer('utf_8')
        
        # Setup Kafka Consumer
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_broker,
            'group.id': 'baggage-routing-group-1',
            'auto.offset.reset': 'latest'
        })
        self.status_topic = "arn.baggage.hub.status_changed"
        self.consumer.subscribe([self.consume_topic, self.status_topic])
        
        # Setup Deserializer for incoming hub/edge status change events
        status_schema = self.sr_client.get_latest_version(self.status_topic + "-value").schema
        self.status_deserializer = AvroDeserializer(self.sr_client, status_schema.schema_str)

        # Setup Kafka Producer
        self.producer = Producer({'bootstrap.servers': self.kafka_broker})
        
    def _handle_status_event(self, msg):
        """Applies incoming hub/edge condition changes to the live routing graph."""
        status_data = self.status_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        node_a = status_data.get("node_a")
        node_b = status_data.get("node_b")
        status = status_data.get("status", "operational")
        congestion_multiplier = status_data.get("congestion_multiplier", 1.0)
        self.engine.builder.update_edge_condition(node_a, node_b, status=status, congestion_multiplier=congestion_multiplier)
        logger.info(f"Applied status update: {node_a} <-> {node_b} = {status} (x{congestion_multiplier})")

    def run(self):
        """The main infinite event loop."""
        logger.info(f"Service running. Listening to topic: {self.consume_topic}")
        
        try:
            while True:
                # Poll the Kafka broker every 1 second
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                # Ingest & Deserialize the Event Payload
                try:
                    if msg.topic() == self.status_topic:
                        self._handle_status_event(msg)
                        continue

                    event_data = self.avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                    
                    # Extract variables based on the Avro contract
                    bag_id = event_data.get("bag_tag_id", "UNKNOWN_BAG")
                    source_node = event_data.get("scanner_location")
                    target_node = event_data.get("destination_node") 
                    
                    logger.info(f"RECEIVED EVENT: BagScanned - ID: {bag_id} at {source_node}")

                    # Process Business Logic
                    route_result = self.engine.calculate_optimal_route(bag_id, source_node, target_node)

                    # Format Output Event
                    if route_result["status"] == "ROUTE_CALCULATED":
                        outbound_event = {
                            "command_id": f"CMD-{bag_id}",
                            "bag_tag_id": bag_id,
                            "target_node": target_node,
                            "path": route_result["path"],
                            "estimated_transit_time_seconds": route_result["metrics"]["estimated_transit_time_seconds"]
                        }
                        
                        # Publish the Command back to Kafka for the hardware to execute
                        self.producer.produce(
                            topic=self.produce_topic,
                            key=self.string_serializer(bag_id),
                            value=self.avro_serializer(outbound_event, SerializationContext(self.produce_topic, MessageField.VALUE))
                        )
                        self.producer.poll(0)
                        logger.info(f"PUBLISHED COMMAND: PLC Routing dispatched for {bag_id}")
                    else:
                        logger.warning(f"Failed to calculate route for {bag_id}. Triggering manual intervention workflow.")
                
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")

        except KeyboardInterrupt:
            logger.info("Service shutting down manually.")
        finally:
            self.consumer.close()
            self.producer.flush()

if __name__ == "__main__":
    service = BaggageRoutingService()
    service.run()