package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SslConfigs;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AivenTopic {

    public static List<String> list() {
        AdminClient adminClient = AivenTopic.createAdminClient();
        ListTopicsResult topicsResult = adminClient.listTopics();

        try {
            return topicsResult.names().get().stream().toList();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Map<String, String>> describe(List<String> topics) {
        AdminClient adminClient = AivenTopic.createAdminClient();
        Map<String, Map<String, String>> topicMap = null;
        try {
            Collection<ConfigResource> crs = topics.stream().map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic)).toList();
            DescribeConfigsResult dc = adminClient.describeConfigs(crs);
            Map<ConfigResource, Config> configs = dc.all().get();
            topicMap = configs
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                                    entry -> entry.getKey().name(),
                                    entry -> {
                                        DescribeTopicsResult tc = adminClient.describeTopics(Collections.singleton(entry.getKey().name()));
                                        String partitionCount;
                                        try {
                                            Map<String, KafkaFuture<TopicDescription>> values = tc.values();
                                            KafkaFuture<TopicDescription> topicDescription = values.get(entry.getKey().name());
                                            partitionCount = String.valueOf(topicDescription.get().partitions().size());
                                        } catch (InterruptedException e) {
                                            throw new RuntimeException(e);
                                        } catch (ExecutionException e) {
                                            throw new RuntimeException(e);
                                        }

                                        Map<String, String> temp = entry.getValue().entries().stream().collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
                                        temp.put("partitions", partitionCount);

                                        return temp;
                                    }
                            )
                    );

            adminClient.close();

        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Noooo!");
        }

        return topicMap;
    }

    private static AdminClient createAdminClient() {
        File truststoreFile = new File("src/main/java/org/example/ca.pem");
        File keyFile = new File("src/main/java/org/example/service.key");
        File certFile = new File("src/main/java/org/example/service.cert");

        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "privatelink-1-kafka-eu-qa-westeu-service-eu-qa.aivencloud.com:11307");
            props.put("security.protocol", "SSL");
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
            props.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, FileUtils.readFileToString(truststoreFile, "UTF-8"));
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
            props.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, FileUtils.readFileToString(keyFile, "UTF-8"));
            props.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, FileUtils.readFileToString(certFile, "UTF-8"));
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 0);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            return AdminClient.create(props);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Integer> partitionCount(String env) {
        Map<String, Integer> partitionMap = null;
        try (AdminClient adminClient = AivenTopic.createAdminClient(env)) {
            ListTopicsResult topics = adminClient.listTopics();
            DescribeTopicsResult tc = adminClient.describeTopics(topics.names().get().stream().toList());
            Map<String, TopicDescription> configs = tc.all().get();

            partitionMap = configs
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            entry -> entry.getValue().name(),
                            entry -> entry.getValue().partitions().size()
                    ));

            return partitionMap;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static AdminClient createAdminClient(String env) {
        String path = "src/main/java/org/example/" + env;
        File truststoreFile = new File(path + "/ca.pem");
        File keyFile = new File(path + "/service.key");
        File certFile = new File(path + "/service.cert");

        String region = null;
        if (Objects.equals(env, "eu-prod")) {
            region = "westeu";
        } else if (Objects.equals(env, "us-prod")) {
            region = "eastus";
        } else {
            region = "krce";
        }
        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "privatelink-1-kafka-" + env + "-" + region + "-service-" + env + ".aivencloud.com:11307");
            props.put("security.protocol", "SSL");
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
            props.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, FileUtils.readFileToString(truststoreFile, "UTF-8"));
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
            props.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, FileUtils.readFileToString(keyFile, "UTF-8"));
            props.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, FileUtils.readFileToString(certFile, "UTF-8"));
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 0);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            return AdminClient.create(props);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}