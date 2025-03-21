package fr.yaon;

import fr.yaon.utils.KafkaUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topicName = "room-temp-measurements-topic";
        int buildingNumbers = 10;
        int minimumRoomNumbers = 10;
        int maximumRoomNumbers = 15;

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AdminClient adminClient = AdminClient.create(config);

        if (!KafkaUtils.topicExists(adminClient, topicName)) {
            KafkaUtils.createTopic(adminClient, topicName, 1, (short) 2);
        }
//        if (!KafkaUtils.topicExists(adminClient, "room-temp-averages")) {
//            KafkaUtils.createTopic(adminClient, "room-temp-averages", 1, (short) 2);
//        }

        TempMeasurementProducer[][] producers = new TempMeasurementProducer[buildingNumbers][];

        for (int buildingID = 0; buildingID < buildingNumbers; buildingID++) {
            int roomNumbers = (int) (Math.random() * (maximumRoomNumbers - minimumRoomNumbers + 1) + minimumRoomNumbers);
            producers[buildingID] = new TempMeasurementProducer[roomNumbers];
            for (int roomID = 0; roomID < roomNumbers; roomID++) {
                producers[buildingID][roomID] = new TempMeasurementProducer(buildingID, roomID, topicName, bootstrapServers);
                producers[buildingID][roomID].start();
            }
        }

        TempMeasurementConsumer consumer = new TempMeasurementConsumer(topicName, bootstrapServers);
        consumer.start();

        try {
            for (TempMeasurementProducer[] buildingProducers : producers) {
                for (TempMeasurementProducer producer : buildingProducers) {
                    producer.join();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
