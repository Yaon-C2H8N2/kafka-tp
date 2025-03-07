package fr.yaon;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TempMeasurementProducer extends Thread {
    private final int buildingID;
    private final int roomID;
    private final String topicName;
    private final String bootstrapServers;

    public TempMeasurementProducer(int buildingID, int roomID, String topicName, String bootstrapServers) {
        this.buildingID = buildingID;
        this.roomID = roomID;
        this.topicName = topicName;
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public void run() {
        double previousTemp = 20.0;
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        try {
            Thread.sleep((long) (Math.random() * 10000));
            while (true) {
                double temp = previousTemp + Math.random() * 5 - 2.5;
                previousTemp = temp;

                String value = new StringBuilder().append(roomID).append(",").append(temp).toString();
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, buildingID, value);
                producer.send(record);

                Thread.sleep(10000);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }
    }
}
