
package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class StreamsApp {
    // Logger for metrics and error reporting
    private static final Logger logger = LoggerFactory.getLogger(StreamsApp.class);

    public static void main(String[] args) {
        // Set up Kafka Streams configuration properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-transactions-streams"); // Unique app ID
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092"); // Kafka broker address
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Key serde
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class); // Value serde (Avro)
        props.put("schema.registry.url", "http://localhost:8081"); // Schema Registry URL

        // Build the stream topology
        StreamsBuilder builder = new StreamsBuilder();

        // Set up Avro Serde for value serialization/deserialization
        final GenericAvroSerde avroSerde = new GenericAvroSerde();
        avroSerde.configure(Collections.singletonMap("schema.registry.url", "http://localhost:8081"), false);

        // Read transactions from the 'bank_transactions' topic (Avro records)
        KStream<String, GenericRecord> transactions = builder.stream("bank_transactions",
                Consumed.with(Serdes.String(), avroSerde));

        // Windowed aggregation: total transaction amount per account every minute
        KTable<Windowed<String>, Double> accountTotals = transactions
            // Group by account field
            .groupBy((key, value) -> value.get("account").toString(), Grouped.with(Serdes.String(), avroSerde))
            // Apply tumbling window of 1 minute
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
            // Aggregate sum of amounts
            .aggregate(
                () -> 0.0, // Initializer
                (key, value, aggregate) -> aggregate + (double) value.get("amount"), // Adder
                Materialized.with(Serdes.String(), Serdes.Double()) // Store type
            );

        // Log windowed totals for metrics
        accountTotals.toStream().foreach((windowedKey, total) -> {
            logger.info("Account {} total in window {}: {}", windowedKey.key(), windowedKey.window().start(), total);
        });

        // Join transactions with customer info from 'customers' topic
        KTable<String, GenericRecord> customers = builder.table("customers",
                Consumed.with(Serdes.String(), avroSerde));
        KStream<String, GenericRecord> enriched = transactions.leftJoin(customers,
            (transaction, customer) -> {
                // Enrich transaction with customer name if available
                if (customer != null) {
                    transaction.put("customer_name", customer.get("name"));
                }
                return transaction;
            }
        );
        // Write enriched transactions to 'enriched_transactions' topic
        enriched.to("enriched_transactions", Produced.with(Serdes.String(), avroSerde));

        // Error handling: filter out negative amounts, log errors, and send to dead-letter topic
        KStream<String, GenericRecord> safeTransactions = transactions.flatMap((key, value) -> {
            try {
                // Simulate error for negative amounts
                if ((double) value.get("amount") < 0) throw new RuntimeException("Negative amount");
                // If OK, pass through
                return Collections.singletonList(KeyValue.pair(key, value));
            } catch (Exception e) {
                // Log error and send to dead-letter topic
                logger.error("Error processing transaction {}: {}", key, e.getMessage());
                // In real case, send to dead-letter topic
                return Collections.singletonList(KeyValue.pair(key, value));
            }
        });
        safeTransactions.to("safe_transactions", Produced.with(Serdes.String(), avroSerde));

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Uncaught exception handler: log and replace thread (could send to dead-letter topic)
        streams.setUncaughtExceptionHandler(ex -> {
            logger.error("Stream error: {}", ex.getMessage());
            // In production, send to dead-letter topic
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        streams.start(); // Start processing
        logger.info("Kafka Streams app started.");

        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
