/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.kafka;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Kafka producer.
 */
public class KafkaReporter extends AbstractReporter implements Scheduled {

	public static final String BROKER_LIST_KEY = "servers";
	public static final String CLIENT_ID_KEY = "id";
	public static final String TOPIC_KEY = "topic";
	public static final String JOBNAME_KEY = "jobName";

	public static final String BROKER_LIST = "bootstrap.servers";
	public static final String CLIENT_ID = "client.id";

	private KafkaProducer<Integer, String> producer;
	private String topic;
	private String jobName;
	private boolean isAsync = true;
	@Override public void open(MetricConfig metricConfig) {
		String brokerList = metricConfig.getString(BROKER_LIST_KEY, null);
		String clientId = metricConfig.getString(CLIENT_ID_KEY, null);
		this.topic = metricConfig.getString(TOPIC_KEY, null);
		this.jobName = metricConfig.getString(JOBNAME_KEY, "default");
		log.info("Kafka metric reporter with brokers: {}, clientId: {}, topic: {}.", brokerList, clientId, topic);
		if (brokerList == null || clientId == null || topic == null) {
			throw new IllegalArgumentException("Invalid borker/clientId/topic configuration.");
		}
		Properties properties = new Properties();
		properties.setProperty(BROKER_LIST, brokerList);
		properties.setProperty(CLIENT_ID, clientId);
		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		this.producer = new KafkaProducer<Integer, String>(properties);
	}

	@Override public void close() {
		producer.close();
	}

	@Override public void report() {
		MetricObject metricObject = new MetricObject(jobName);
		for (Map.Entry<Counter, String> metric : counters.entrySet()) {
			metricObject.counters.put(metric.getValue(), metric.getKey().getCount() + "");
		}

		for (Map.Entry<Gauge<?>, String> metric : gauges.entrySet()) {
			metricObject.gauges.put(metric.getValue(), metric.getKey().getValue() + "");
		}

		for (Map.Entry<Meter, String> metric : meters.entrySet()) {
			metricObject.meters.put(metric.getValue(), metric.getKey().getRate() + "");
		}

		for (Map.Entry<Histogram, String> metric : histograms.entrySet()) {
			HistogramStatistics stats = metric.getKey().getStatistics();
			metricObject.histograms.put(metric.getValue() + ".mean", stats.getMean() + "");
		}
		String jsonString = JSON.toJSONString(metricObject);
		producer.send(new ProducerRecord<>(topic, jsonString));

	}

	@Override public String filterCharacters(String input) {
		return input;
	}

	private static class MetricObject implements Serializable {
		public Map<String, String> counters = new HashMap<>();
		public Map<String, String> gauges = new HashMap<>();
		public Map<String, String> meters = new HashMap<>();
		public Map<String, String> histograms = new HashMap<>();
		public String jobName;

		public MetricObject() {}

		public MetricObject(String jobName) {
			this.jobName = jobName;
		}
	}
}
