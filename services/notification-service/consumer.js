const { Kafka } = require('kafkajs');
const { SchemaRegistryClient, SerdeType, AvroDeserializer } = require('@confluentinc/schemaregistry');

// ---------------------------------------------------------
// JSON Logger Configuration
// ---------------------------------------------------------
const logger = {
    info: (msg, meta = {}) => {
        console.log(JSON.stringify({
            timestamp: new Date().toISOString(),
            level: "INFO",
            service: "notification-consumer",
            message: msg,
            ...meta
        }));
    },
    error: (msg, meta = {}) => {
        console.error(JSON.stringify({
            timestamp: new Date().toISOString(),
            level: "ERROR",
            service: "notification-consumer",
            message: msg,
            ...meta
        }));
    }
};

// ---------------------------------------------------------
// Application Configuration
// ---------------------------------------------------------
const KAFKA_BROKER = 'localhost:9092';
const SCHEMA_REGISTRY_URL = 'http://localhost:8081';
const TOPIC_NAME = 'arn.flight.status.updated';

async function main() {
    logger.info("Initializing Notification Consumer service...");

    // Initialize Schema Registry Client Kafka Client and Consumer
    const registry = new SchemaRegistryClient({ baseURLs: [SCHEMA_REGISTRY_URL] });
    const deserializer = new AvroDeserializer(registry, SerdeType.VALUE, {});

    const kafka = new Kafka({
        clientId: 'notification-service',
        brokers: [KAFKA_BROKER]
    });

    // Consumers sharing a groupId act as a cluster. (horizontal scaling)
    const consumer = kafka.consumer({ groupId: 'notification-group' });

    try {
        await consumer.connect();
        logger.info("Successfully connected to Kafka broker.");

        // fromBeginning: true ensures we read old messages if the consumer starts late
        await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });
        logger.info(`Subscribed to topic: ${TOPIC_NAME}`);

        // Consume and Decode the Event Stream
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    // The AvroDeserializer fetches the schema and unpacks the binary payload back into a standard JS object
                    const decodedValue = await deserializer.deserialize(TOPIC_NAME, message.value);
                    
                    logger.info("Intercepted and decoded flight event!", {
                        partition: partition,
                        flight_number: decodedValue.data.flight_number,
                        status: decodedValue.data.status,
                        correlation_id: decodedValue.correlation_id
                    });
                } catch (decodeError) {
                    logger.error("Failed to decode message payload", { error: decodeError.message });
                }
            },
        });
    } catch (error) {
        logger.error("Fatal error in consumer operation", { error: error.message });
    }
}

// Handle shutdowns
process.on('SIGINT', async () => {
    logger.info("Initiating shutdown...");
    try {
        await consumer.disconnect();
        logger.info("Successfully disconnected from Kafka broker.");
        process.exit(0);
    } catch (shutdownError) {
        logger.error("Error during shutdown", { error: shutdownError.message });
        process.exit(1);
    }
});

main();