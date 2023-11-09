import org.example.TopicRegexFilter;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;


import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class RegexTest {
    @Test
    public void testExactMatch() {
        List<String> topicRegexes = List.of(
                "internal.de.replicator.test-topic"
        );
        List<String> allTopics = List.of(
                "internal.de.replicator.test-topic",
                "internal.de.replicator.wat",
                "blip",
                "blop"
        );
        List<String> expected = List.of("internal.de.replicator.test-topic");
        List<String> actual = TopicRegexFilter.filter(topicRegexes, allTopics);

        assertThat(actual, is(expected));
    }

    @Test
    public void testRegexMatch() {
        List<String> topicRegexes = List.of(
                "internal.de.replicator.*"
        );
        List<String> allTopics = List.of(
                "internal.de.replicator.test-topic",
                "internal.de.replicator.wat",
                "blip",
                "blop"
        );
        List<String> expected = List.of(
                "internal.de.replicator.test-topic",
                "internal.de.replicator.wat"
        );
        List<String> actual = TopicRegexFilter.filter(topicRegexes, allTopics);

        assertThat(actual, is(expected));
    }

    @Test
    public void testMultipleRegexMatch() {
        List<String> topicRegexes = List.of(
                "internal.de.replicator.*",
                "internal.de.keplicator.*"
        );
        List<String> allTopics = List.of(
                "internal.de.replicator.test-topic",
                "internal.de.replicator.wat",
                "internal.de.keplicator.kek",
                "blip",
                "blop"
        );
        List<String> expected = List.of(
                "internal.de.replicator.test-topic",
                "internal.de.replicator.wat",
                "internal.de.keplicator.kek"
        );
        List<String> actual = TopicRegexFilter.filter(topicRegexes, allTopics);

        assertThat(actual, is(expected));
    }

    @Test
    public void testPrefixRegexMatch() {
        List<String> topicRegexes = List.of(
                ".*.de.replicator.test-topic"
        );
        List<String> allTopics = List.of(
                "internal.de.replicator.test-topic",
                "lol.de.replicator.test-topic",
                "internal.de.replicator.wat",
                "internal.de.keplicator.kek",
                "blip",
                "blop"
        );
        List<String> expected = List.of(
                "internal.de.replicator.test-topic",
                "lol.de.replicator.test-topic"
        );
        List<String> actual = TopicRegexFilter.filter(topicRegexes, allTopics);

        assertThat(actual, is(expected));
    }

    @Test
    public void testRegexMiddleMatch() {
        List<String> topicRegexes = List.of(
                "internal.*.replicator.test-topic"
        );
        List<String> allTopics = List.of(
                "internal.de.replicator.test-topic",
                "internal.se.replicator.test-topic",
                "internal.ke.replicator.test-topic",
                "blip",
                "blop"
        );
        List<String> expected = List.of(
                "internal.de.replicator.test-topic",
                "internal.se.replicator.test-topic",
                "internal.ke.replicator.test-topic"
        );
        List<String> actual = TopicRegexFilter.filter(topicRegexes, allTopics);

        assertThat(actual, is(expected));
    }
}