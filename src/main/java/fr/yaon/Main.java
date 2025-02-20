package fr.yaon;
import org.apache.kafka.clients.admin.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092"; // Change this if needed
        String topicName = "my_new_topic";
        int numPartitions = 2;
        short replicationFactor = 2;

        // Create admin client properties
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Create Kafka AdminClient
        try (AdminClient adminClient = AdminClient.create(config)) {
            // Create new topic
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

            // Ensure the topic is created
            result.all().get();
            System.out.println("Topic created successfully: " + topicName);
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Error creating topic: " + e.getMessage());
        }
    }
}
