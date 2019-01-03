package io.wizzie.metrics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class KafkaMetricsTest {
    private static final String METRICS_TOPIC = "metrics";
    static String jarFile;

    static {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File fileAux = new File(classLoader.getResource("log4j2.xml").getFile());

        for (File file : fileAux.getParentFile().listFiles()) {
            if (file.getName().endsWith("-selfcontained.jar")) {
                jarFile = file.getAbsolutePath();
            }
        }
    }

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer("5.1.0")
            .withEmbeddedZookeeper()
            .withEnv("KAFKA_KAFKA_METRICS_REPORTERS", KafkaBrokerReporter.class.getName())
            .withEnv("KAFKA_KAFKA_METRICS_POLLING_INTERVAL_SECS", "1")
            .withEnv("KAFKA_METRICS_REPORTER_KAFKA_BROKER_REPORTER_TOPIC", METRICS_TOPIC)
            .withEnv("KAFKA_METRICS_REPORTER_KAFKA_BROKER_REPORTER_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(jarFile),
                    "/usr/share/java/kafka/kafka-metric-reporter-selfcontained.jar"
            );

    @Test
    public void checkThatReceiveSomeMetric() throws InterruptedException {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "group-metrics-test");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<KeyValue<String, String>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, METRICS_TOPIC, 1);

        assertTrue(result.size() > 0);
        assertTrue(result.get(0).value.contains("host"));
        assertTrue(result.get(0).value.contains("component"));
        assertTrue(result.get(0).value.contains("timestamp"));
        assertTrue(result.get(0).value.contains("value"));
        assertTrue(result.get(0).value.contains("monitor"));
    }
}
