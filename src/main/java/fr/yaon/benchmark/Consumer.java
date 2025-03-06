package fr.yaon.benchmark;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class Consumer extends Thread {
    private final String bootstrapServers;
    private final String topicName;
    private final String consumerGroup;
    private final Map<String, String> messages;

    Consumer(String topicName, String bootstrapServers, String consumerGroup, Map<String, String> messages) {
        this.bootstrapServers = bootstrapServers;
        this.topicName = topicName;
        this.consumerGroup = consumerGroup;
        this.messages = messages;
    }

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topicName));
        while (consumer.assignment().isEmpty()) {
            consumer.poll(Duration.ofMillis(100));
        }
        consumer.assignment().forEach(partition -> consumer.seek(partition, 0));

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());

        try {
            boolean isOver = false;
            while (!isOver) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String message = String.format("Received message: key=%s, value=%s, partition=%d, offset=%d",
                            record.key(), record.value(), record.partition(), record.offset());
                    messages.put(UUID.randomUUID().toString(), message);
                }

                isOver = consumer.assignment().stream()
                        .noneMatch(partition -> consumer.position(partition) < endOffsets.get(partition));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
