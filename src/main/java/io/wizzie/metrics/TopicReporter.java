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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class TopicReporter extends AbstractPollingReporter implements MetricProcessor<String> {
    private final MetricPredicate predicate = MetricPredicate.ALL;
    Producer<String, String> producer;
    ObjectMapper mapper = new ObjectMapper();
    String brokerId;
    String topic;

    public TopicReporter(MetricsRegistry metricsRegistry, Properties producerConfig, String brokerId, String topic) {
        super(metricsRegistry, "kafka-topic-reporter");
        this.brokerId = brokerId;
        this.topic = topic;
        producer = new KafkaProducer(producerConfig);
    }

    public void run() {
        final Set<Map.Entry<MetricName, Metric>> metrics = getMetricsRegistry().allMetrics().entrySet();
        try {
            for (Map.Entry<MetricName, Metric> entry : metrics) {
                final MetricName metricName = entry.getKey();
                final Metric metric = entry.getValue();
                if (predicate.matches(metricName, metric)) {
                    metric.processWith(this, metricName, null);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void processMeter(MetricName name, Metered meter, String context) {
        Map<String, Object> data = new HashMap<>();

        String monitor;
        if (name.getScope() != null) {
            monitor = name.getName() + "-" + name.getScope();
        } else {
            monitor = name.getName();
        }

        data.put("app_id", "kafka");
        data.put("component", name.getGroup());
        data.put("monitor", monitor + "-1minute");
        data.put("value", meter.oneMinuteRate());
        data.put("timestamp", System.currentTimeMillis() / 1000L);
        data.put("host", brokerId);

        send(data);
    }

    public void processCounter(MetricName name, Counter counter, String context) {
        Map<String, Object> data = new HashMap<>();

        String monitor;
        if (name.getScope() != null) {
            monitor = name.getName() + "-" + name.getScope();
        } else {
            monitor = name.getName();
        }

        data.put("app_id", "kafka");
        data.put("component", name.getGroup());
        data.put("monitor", monitor);
        data.put("value", counter.count());
        data.put("timestamp", System.currentTimeMillis() / 1000L);
        data.put("host", brokerId);

        send(data);
    }

    public void processHistogram(MetricName name, Histogram histogram, String context) {
        Map<String, Object> data = new HashMap<>();

        String monitor;
        if (name.getScope() != null) {
            monitor = name.getName() + "-" + name.getScope();
        } else {
            monitor = name.getName();
        }

        data.put("app_id", "kafka");
        data.put("component", name.getGroup());
        data.put("monitor", monitor);
        data.put("value", histogram.mean());
        data.put("timestamp", System.currentTimeMillis() / 1000L);
        data.put("host", brokerId);

        send(data);
    }

    public void processTimer(MetricName name, Timer timer, String context) {
        Map<String, Object> data = new HashMap<>();

        String monitor;
        if (name.getScope() != null) {
            monitor = name.getName() + "-" + name.getScope();
        } else {
            monitor = name.getName();
        }

        data.put("app_id", "kafka");
        data.put("component", name.getGroup());
        data.put("monitor", monitor);
        data.put("value", timer.mean());
        data.put("timestamp", System.currentTimeMillis() / 1000L);
        data.put("host", brokerId);

        send(data);
    }

    public void processGauge(MetricName name, Gauge<?> gauge, String context) {
        Map<String, Object> data = new HashMap<>();

        String monitor;
        if (name.getScope() != null) {
            monitor = name.getName() + "-" + name.getScope();
        } else {
            monitor = name.getName();
        }

        data.put("app_id", "kafka");
        data.put("component", name.getGroup());
        data.put("monitor", monitor);
        data.put("value", gauge.value().toString());
        data.put("timestamp", System.currentTimeMillis() / 1000L);
        data.put("host", brokerId);

        send(data);
    }

    @Override
    public void start(long period, TimeUnit unit) {
        super.start(period, unit);
    }

    @Override
    public void shutdown() {
        try {
            super.shutdown();
        } finally {
            producer.close();
        }
    }

    private void send(Map<String, Object> message) {
        try {
            producer.send(new ProducerRecord(topic, mapper.writeValueAsString(message)));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}