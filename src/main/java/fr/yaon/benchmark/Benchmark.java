package fr.yaon.benchmark;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.text.NumberFormat;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class Benchmark {
    private static void createTopic(AdminClient adminClient, String topicName, int numPartitions, short replicationFactor) {
        try {
            System.out.println("Creating topic: " + topicName);
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

            result.all().get();
        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Error creating topic: " + e.getMessage());
        }
    }

    private static void initBenchmark(Properties adminClientConfig, String bootstrapServers, String topicName, int numPartitions, short replicationFactor, int producerCount) {
        AdminClient adminClient = AdminClient.create(adminClientConfig);

        try {
            if (adminClient.listTopics().names().get().contains(topicName)) {
                System.out.println("Topic already exists: " + topicName);
                return;
            }
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error listing topics: " + e.getMessage());
            return;
        }

        createTopic(adminClient, topicName, numPartitions, replicationFactor);
        createMessages(bootstrapServers, producerCount, topicName);
    }

    private static void createMessages(String bootstrapServers, int producerCount, String topicName) {
        int messageCount = 100_000;
        final Producer[] producers = new Producer[producerCount];

        System.out.println("Producing " + NumberFormat.getInstance().format(messageCount * producerCount) + " messages");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < producers.length; i++) {
            producers[i] = new Producer(topicName, bootstrapServers, messageCount);
            producers[i].start();
        }
        for (Producer producer : producers) {
            try {
                producer.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken to produce messages: " + (endTime - startTime) + "ms");
    }

    private static void benchmark(int consumerCount, int groupCount, String topicName, String bootstrapServers) {
        final Consumer[] consumers = new Consumer[consumerCount];
        ConcurrentHashMap<String, String> messages = new ConcurrentHashMap<>();

        System.out.println("Consuming messages with " + consumerCount + " consumers and " + groupCount + " groups");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < consumers.length; i++) {
            String consumerGroup = "benchmark_group_" + (i % groupCount);
            consumers[i] = new Consumer(topicName, bootstrapServers, consumerGroup, messages);

            consumers[i].start();
        }
        for (Consumer consumer : consumers) {
            try {
                consumer.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Time taken to consume messages: " + (endTime - startTime) + "ms");
        System.out.println("Messages consumed: " + NumberFormat.getInstance().format(messages.size()));
        int randomIndex = (int) (Math.random() * messages.size());
        System.out.println("Random message preview: " + messages.values().toArray()[randomIndex]);
    }

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topicName = "benchmark-topic";
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        initBenchmark(config, bootstrapServers, topicName, 2, (short) 2, 10);

        benchmark(2, 1, topicName, bootstrapServers);
        System.out.println();
        benchmark(3, 1, topicName, bootstrapServers);
        System.out.println();
        benchmark(2, 2, topicName, bootstrapServers);
    }
}
