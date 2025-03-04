package fr.yaon.benchmark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class Producer extends Thread{
    private String bootstrapServers;
    private String topicName;
    private int messageCount;
    private UUID uuid;

    Producer(String topicName, String bootstrapServers, int messageCount) {
        this.bootstrapServers = bootstrapServers;
        this.topicName = topicName;
        this.messageCount = messageCount;
        this.uuid = UUID.randomUUID();
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < messageCount; i++) {
                String key = "Key-" + this.uuid.toString() +  "-" + i;
                String value = "Message-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
