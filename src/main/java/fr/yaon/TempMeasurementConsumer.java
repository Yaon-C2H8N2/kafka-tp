package fr.yaon;

import fr.yaon.utils.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class TempMeasurementConsumer extends Thread {
    private final String topicName;
    private final String bootstrapServers;

    public TempMeasurementConsumer(String topicName, String bootstrapServers) {
        this.topicName = topicName;
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "temp-measurement-consumer");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.put("auto.offset.reset", "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, String> source = builder.stream(topicName);

        KStream<String, Double> roomTemperatures = source.flatMap((buildingId, value) -> {
            String[] parts = value.split(",");
            if (parts.length != 2) {
                return new ArrayList<>();
            }
            String roomId = parts[0].trim();
            double temperature = Double.parseDouble(parts[1].trim());
            String compositeKey = String.format("%d,%s", buildingId, roomId);
            return Collections.singletonList(KeyValue.pair(compositeKey, temperature));
        });

        TimeWindows fiveMinuteWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5));

        KTable<Windowed<String>, TemperatureData> aggregatedTable = roomTemperatures
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(fiveMinuteWindow)
                .aggregate(
                        () -> new TemperatureData(),
                        (room, temperature, agg) -> agg.addTemperature(temperature),
                        Materialized.with(Serdes.String(), new JsonSerdes<TemperatureData>())
                );

        aggregatedTable.mapValues(aggregate ->
                    aggregate.computeAverageTemperature()
                ).toStream()
                .foreach((windowedKey, avg) -> {
                    String key = windowedKey.key();
                    String windowStart = Date.from(Instant.ofEpochMilli(windowedKey.window().start())).toString();
                    String windowEnd = Date.from(Instant.ofEpochMilli(windowedKey.window().end())).toString();
//                    System.out.printf("Key: %s, Window: [%s, %s), Average: %.2f%n", key, windowStart, windowEnd, avg);
                    if(avg < 15 || avg > 25) {
                        System.out.printf("Key: %s, Window: [%s, %s), Average: %.2f%n", key, windowStart, windowEnd, avg);
                    }
                });

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        System.out.println("Starting the Kafka Streams");
        streams.start();
        System.out.println("Started the Kafka Streams");
    }
}
