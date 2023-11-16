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