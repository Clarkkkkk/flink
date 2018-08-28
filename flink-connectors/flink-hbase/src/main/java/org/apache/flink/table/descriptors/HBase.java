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

package org.apache.flink.table.descriptors;

import org.apache.flink.configuration.MemorySize;

import java.util.Map;

import static org.apache.flink.streaming.connectors.hbase.HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_ENABLE;
import static org.apache.flink.streaming.connectors.hbase.HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_INTERVAL_MS;
import static org.apache.flink.streaming.connectors.hbase.HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_MUTATIONS;
import static org.apache.flink.streaming.connectors.hbase.HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_RETRIES;
import static org.apache.flink.streaming.connectors.hbase.HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_SIZE_MB;
import static org.apache.flink.streaming.connectors.hbase.HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_TIMEOUT_MS;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_CF_QUALIFIER_DELIMITER;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_CLUSTER_KEY;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_ROW_KEY;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TABLE_NAME;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TYPE_VALUE_HBASE;

/**
 * Connector descriptor for the HBase database.
 */
public class HBase extends ConnectorDescriptor {

	private DescriptorProperties internalProperties = new DescriptorProperties(true);

	/**
	 * Connector descriptor for the HBase database.
	 */
	public HBase() {
		super(CONNECTOR_TYPE_VALUE_HBASE, 1, false);
	}

	/**
	 * Adds an HBase cluster key to connect to. Required.
	 * @param clusterKey connection cluster key
	 * @return
	 */
	public HBase clusterKey(String clusterKey) {
		internalProperties.putString(CONNECTOR_CLUSTER_KEY, clusterKey);
		return this;
	}

	/**
	 * Adds an HBase table to connect to. Required.
	 * @param tableName table name
	 * @return
	 */
	public HBase tableName(String tableName) {
		internalProperties.putString(CONNECTOR_TABLE_NAME, tableName);
		return this;
	}

	/**
	 * Configures the field used as row key.
	 * @param rowKey
	 * @return
	 */
	public HBase rowKey(String rowKey) {
		internalProperties.putString(CONNECTOR_ROW_KEY, rowKey);
		return this;
	}

	/**
	 * Configures the delimiter to apply on field name to seperate the column family and qualifier. Default is ":".
	 * @param delimiter
	 * @return
	 */
	public HBase delimiter(String delimiter) {
		internalProperties.putString(CONNECTOR_CF_QUALIFIER_DELIMITER, delimiter);
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in a batch to the cluster for efficiency.
	 *
	 * <p>Enable the batch flush for mutations.
	 *
	 */
	public HBase enableBatchFlush() {
		internalProperties.putBoolean(CONFIG_KEY_BATCH_FLUSH_ENABLE, true);
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in a batch to the cluster for efficiency.
	 *
	 * <p>Sets the maximum number of mutations to buffer for each batch request.
	 *
	 * @param maxMutations the maximum number of mutations to buffer per batch.
	 */
	public HBase batchFlushMaxMutations(int maxMutations) {
		internalProperties.putInt(CONFIG_KEY_BATCH_FLUSH_MAX_MUTATIONS, maxMutations);
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in a batch to the cluster for efficiency.
	 *
	 * <p>Sets the maximum size of buffered mutations per batch (using the syntax of {@link MemorySize}).
	 *
	 * @param maxSize the maximum size of mutations to buffer per batch.
	 */
	public HBase batchFlushMaxSizeMb(String maxSize) {
		internalProperties.putMemorySize(CONFIG_KEY_BATCH_FLUSH_MAX_SIZE_MB, MemorySize.parse(maxSize, MemorySize.MemoryUnit.BYTES));
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in a batch to the cluster for efficiency.
	 *
	 * <p>Sets the batch flush interval (in milliseconds).
	 *
	 * @param interval batch flush interval (in milliseconds).
	 */
	public HBase batchFlushInterval(long interval) {
		internalProperties.putLong(CONFIG_KEY_BATCH_FLUSH_INTERVAL_MS, interval);
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in a batch to the cluster for efficiency.
	 *
	 * <p>Sets the maximum number of retries for a batch.
	 *
	 * @param retries batch flush retries.
	 */
	public HBase batchFlushMaxRetries(int retries) {
		internalProperties.putInt(CONFIG_KEY_BATCH_FLUSH_MAX_RETRIES, retries);
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in a batch to the cluster for efficiency.
	 *
	 * <p>Sets the maximum timeout for a batch(in milliseconds).
	 *
	 * @param timeout batch flush maximum timeout (in milliseconds).
	 */
	public HBase batchFlushMaxTimeout(long timeout) {
		internalProperties.putLong(CONFIG_KEY_BATCH_FLUSH_MAX_TIMEOUT_MS, timeout);
		return this;
	}

	@Override protected Map<String, String> toConnectorProperties() {
		return internalProperties.asMap();
	}
}
