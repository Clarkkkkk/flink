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

package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.HBaseValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_CF_QUALIFIER_DELIMITER;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_CLUSTER_KEY;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_ROW_KEY;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TABLE_NAME;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TYPE_VALUE_HBASE;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_TYPE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/**
 * Table factory for creating an {@link org.apache.flink.table.sinks.UpsertStreamTableSink} for HBase.
 */
@Internal
public class HBaseUpsertTableSinkFactory implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	public static final String DEFAULT_CF_QUALIFIER_DELIMITER = ":";

	@Override public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		final String delimiter = descriptorProperties.getOptionalString(CONNECTOR_CF_QUALIFIER_DELIMITER)
			.orElse(DEFAULT_CF_QUALIFIER_DELIMITER);
		return new HBaseUpsertTableSink(
			descriptorProperties.isValue(UPDATE_MODE(), UPDATE_MODE_VALUE_APPEND()),
			descriptorProperties.getTableSchema(SCHEMA()),
			descriptorProperties.getString(CONNECTOR_CLUSTER_KEY),
			descriptorProperties.getString(CONNECTOR_TABLE_NAME),
			getUserConfig(descriptorProperties),
			descriptorProperties.getString(CONNECTOR_ROW_KEY),
			delimiter
		);
	}

	@Override public Map<String, String> requiredContext() {
		final Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_HBASE);
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override public List<String> supportedProperties() {
		final List<String> properties = new ArrayList<>();

		// streaming properties
		properties.add(UPDATE_MODE());

		// Hbase
		properties.add(CONNECTOR_CLUSTER_KEY);
		properties.add(CONNECTOR_TABLE_NAME);
		properties.add(CONNECTOR_ROW_KEY);
		properties.add(CONNECTOR_CF_QUALIFIER_DELIMITER);
		properties.add(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_ENABLE);
		properties.add(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_MUTATIONS);
		properties.add(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_SIZE_MB);
		properties.add(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_INTERVAL_MS);
		properties.add(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_RETRIES);
		properties.add(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_TIMEOUT_MS);

		// schema
		properties.add(SCHEMA() + ".#." + SCHEMA_TYPE());
		properties.add(SCHEMA() + ".#." + SCHEMA_NAME());
		// format wildcard

		return properties;
	}

	// --------------------------------------------------------------------------------------------
	// Helper methods
	// --------------------------------------------------------------------------------------------

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new StreamTableDescriptorValidator(true, true, true).validate(descriptorProperties);
		new SchemaValidator(true, false, false).validate(descriptorProperties);
		new HBaseValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	private Map<String, String> getUserConfig(DescriptorProperties descriptorProperties) {
		Map<String, String> userConfig = new HashMap<>();
		descriptorProperties.getOptionalBoolean(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_ENABLE)
			.ifPresent(v -> userConfig.put(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_ENABLE, v.toString()));
		descriptorProperties.getOptionalLong(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_MUTATIONS)
			.ifPresent(v -> userConfig.put(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_MUTATIONS, v.toString()));
		descriptorProperties.getOptionalMemorySize(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_SIZE_MB)
			.ifPresent(v -> userConfig.put(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_SIZE_MB, String.valueOf(v.getMebiBytes())));
		descriptorProperties.getOptionalLong(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_INTERVAL_MS)
			.ifPresent(v -> userConfig.put(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_INTERVAL_MS, String.valueOf(v)));
		descriptorProperties.getOptionalInt(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_RETRIES)
			.ifPresent(v -> userConfig.put(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_RETRIES, String.valueOf(v)));
		descriptorProperties.getOptionalLong(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_TIMEOUT_MS)
			.ifPresent(v -> userConfig.put(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_TIMEOUT_MS, String.valueOf(v)));
		return userConfig;
	}
}
