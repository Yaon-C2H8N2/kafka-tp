package fr.yaon;

import fr.yaon.utils.Pair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Properties;

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

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, String> source = builder.stream(topicName);

        KStream<String, Double> roomTemperatures = source.flatMap((buildingId, value) -> {
            String[] parts = value.split(",");
            if (parts.length != 2) {
                return new java.util.ArrayList<>();
            }
            String roomId = parts[0].trim();
            double temperature = Double.parseDouble(parts[1].trim());
            String compositeKey = String.format("%d,%s", buildingId, roomId);
            return java.util.Collections.singletonList(KeyValue.pair(compositeKey, temperature));
        });

        KTable<Windowed<String>, HashMap<String, Pair<Double, Integer>>> aggregated = roomTemperatures
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .aggregate(
                        HashMap::new,
                        (compositeKey, temperature, aggregate) -> {
                            Pair<Double, Integer> temperatures = aggregate.get(compositeKey);

                            if (temperatures == null) {
                                temperatures = new Pair<>(temperature, 1);
                            } else {
                                temperatures.setValue1(temperatures.getValue1() + temperature);
                                temperatures.setValue2(temperatures.getValue2() + 1);
                            }

                            aggregate.put(compositeKey, temperatures);
                            return aggregate;
                        }
                );

        aggregated.toStream().foreach((windowedKey, stats) -> {
            String[] compositeKey = windowedKey.key().split(",");
            int buildingId = Integer.parseInt(compositeKey[0]);
            int roomId = Integer.parseInt(compositeKey[1]);
            Instant windowStart = windowedKey.window().startTime();
            Instant windowEnd = windowedKey.window().endTime();
            double average = stats.get(windowedKey.key()).getValue1() / stats.get(windowedKey.key()).getValue2();

            //todo return the average temperature for the room in the last 5 minutes
            System.out.println(String.format("Building %d, Room %d, Window start %s, Window end %s, Average temperature %f",
                    buildingId, roomId, windowStart, windowEnd, average));
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        System.out.println("Starting the Kafka Streams");
        streams.start();
        System.out.println("Started the Kafka Streams");
    }
}
