/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.wizzie.metrics;

import com.yammer.metrics.Metrics;
import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.metrics.KafkaMetricsReporterMBean;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class KafkaBrokerReporter implements KafkaMetricsReporter, KafkaBrokerReporterMBean {
    public final static String METRIC_PREFIX = "kafka.metrics.reporter.kafka.broker.reporter.";
    private static final Logger log = LoggerFactory.getLogger(KafkaBrokerReporter.class);
    private TopicReporter underlying;
    private VerifiableProperties props;
    private boolean running;
    private boolean initialized;

    @Override
    public String getMBeanName() {
        return "kafka:type=io.wizzie.metrics.KafkaBrokerReporter";
    }

    synchronized public void init(VerifiableProperties props) {
        if (!initialized) {
            this.props = props;
            props.props().put(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    props.getString(
                            METRIC_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                            "localhost:9092"
                    )
            );
            props.props().put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.props().put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            final KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);

            this.underlying = new TopicReporter(
                    Metrics.defaultRegistry(),
                    props.props(),
                    "kafka-" + props.getString("broker.id"),
                    props.getString(METRIC_PREFIX + "topic", "metrics")
            );
            initialized = true;
            startReporter(metricsConfig.pollingIntervalSecs());
        }
    }

    synchronized public void startReporter(long pollingPeriodSecs) {
        if (initialized && !running) {
            underlying.start(pollingPeriodSecs, TimeUnit.SECONDS);
            running = true;
            log.info(String.format("Started Kafka Topic metrics reporter with polling period %d seconds", pollingPeriodSecs));
        }
    }

    public synchronized void stopReporter() {
        if (initialized && running) {
            underlying.shutdown();
            running = false;
            log.info("Stopped Kafka Topic metrics reporter");
            underlying = new TopicReporter(
                    Metrics.defaultRegistry(),
                    props.props(),
                    String.format("broker%s", props.getString("broker.id")),
                    props.getString(METRIC_PREFIX + "topic", "metrics")
            );
        }
    }
}
