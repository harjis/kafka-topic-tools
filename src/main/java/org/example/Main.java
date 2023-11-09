package org.example;

import java.util.List;
import java.util.Map;

public class Main {
    static List<String> ignoredTopicRegexes = List.of(".*.northeu.*");

    public static void main(String[] args) {
        int euProdPartitionCount = Main.euProdPartitionCount();
        int usProdPartitionCount = Main.usProdPartitionCount();
        int apacProdPartitionCount = Main.apacProdPartitionCount();

        System.out.println("us-prod partition count:" + usProdPartitionCount);
        System.out.println("eu-prod partition count:" + euProdPartitionCount);
        System.out.println("apac-prod partition count:" + apacProdPartitionCount);
    }

    private static Integer euProdPartitionCount(){
        Map<String, Integer> aivenTopicConfigs = AivenTopic.partitionCount("eu-prod");

        return aivenTopicConfigs
                .entrySet()
                .stream()
                .filter(stringIntegerEntry -> ignoredTopicRegexes
                        .stream()
                        .noneMatch(stringIntegerEntry.getKey()::matches))
                .map(Map.Entry::getValue)
                .reduce(0, Integer::sum);
    }

    private static Integer usProdPartitionCount(){
        Map<String, Integer> aivenTopicConfigs = AivenTopic.partitionCount("us-prod");

        return aivenTopicConfigs
                .entrySet()
                .stream()
                .filter(stringIntegerEntry -> ignoredTopicRegexes
                        .stream()
                        .noneMatch(stringIntegerEntry.getKey()::matches))
                .map(Map.Entry::getValue)
                .reduce(0, Integer::sum);
    }

    private static Integer apacProdPartitionCount(){
        Map<String, Integer> aivenTopicConfigs = AivenTopic.partitionCount("apac-prod");

        return aivenTopicConfigs
                .entrySet()
                .stream()
                .filter(stringIntegerEntry -> ignoredTopicRegexes
                        .stream()
                        .noneMatch(stringIntegerEntry.getKey()::matches))
                .map(Map.Entry::getValue)
                .reduce(0, Integer::sum);
    }
}
