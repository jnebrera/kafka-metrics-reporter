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

import com.codahale.metrics.*;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class KafkaReporter extends ScheduledReporter {
    private static final Logger log = LoggerFactory.getLogger(KafkaReporter.class);

    private final Producer<String, String> kafkaProducer;
    private final String kafkaTopic;
    private final ObjectMapper mapper;
    private final MetricRegistry registry;

    private KafkaReporter(MetricRegistry registry,
                          String name,
                          MetricFilter filter,
                          TimeUnit rateUnit,
                          TimeUnit durationUnit,
                          String kafkaTopic,
                          Properties kafkaProperties) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.registry = registry;
        mapper = new ObjectMapper().registerModule(new MetricsModule(rateUnit,
                durationUnit,
                false));
        this.kafkaTopic = kafkaTopic;
        kafkaProducer = new KafkaProducer<>(kafkaProperties);
    }

    @Override
    public synchronized void report(SortedMap<String, Gauge> gauges,
                                    SortedMap<String, Counter> counters,
                                    SortedMap<String, Histogram> histograms,
                                    SortedMap<String, Meter> meters,
                                    SortedMap<String, Timer> timers) {
        try {
            log.info("Trying to report metrics to Kafka kafkaTopic {}", kafkaTopic);
            StringWriter report = new StringWriter();
            mapper.writeValue(report, registry);
            log.debug("Created metrics report: {}", report);
            kafkaProducer.send(new ProducerRecord<>(kafkaTopic, report.toString()));
            log.info("Metrics were successfully reported to Kafka kafkaTopic {}", kafkaTopic);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
