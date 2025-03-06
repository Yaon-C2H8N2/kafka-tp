package fr.yaon.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class KafkaUtils {
    public static void createTopic(AdminClient adminClient, String topicName, int numPartitions, short replicationFactor) {
        try {
            System.out.println("Creating topic: " + topicName);
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

            result.all().get();
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Error creating topic: " + e.getMessage());
        }
    }

    public static Boolean topicExists(AdminClient adminClient, String topicName) {
        try {
            if (adminClient.listTopics().names().get().contains(topicName)) {
                return true;
            }
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error listing topics: " + e.getMessage());
            return false;
        }
        return false;
    }
}
