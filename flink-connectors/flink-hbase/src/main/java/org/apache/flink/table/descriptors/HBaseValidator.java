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

import org.apache.flink.streaming.connectors.hbase.HBaseSinkFunctionBase;
import org.apache.flink.table.api.ValidationException;

/**
 * The Validator for {@link HBase}.
 */
public class HBaseValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_HBASE = "hbase";
	public static final String CONNECTOR_CLUSTER_KEY = "connector.cluster-key";
	public static final String CONNECTOR_TABLE_NAME = "connector.table-name";
	public static final String CONNECTOR_ROW_KEY = "connector.row-key";
	public static final String CONNECTOR_CF_QUALIFIER_DELIMITER = "connector.cf-qualifier-delimiter";

	@Override public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_HBASE, false);
		validateClusterKey(properties);
		validateGeneralProperties(properties);
		validateBatchFlushProperties(properties);
	}

	private void validateClusterKey(DescriptorProperties properties) {
		String clusterKey = properties.getString(CONNECTOR_CLUSTER_KEY);
		if (clusterKey == null) {
			throw new ValidationException("ClusterKey is null.");
		}
		String[] segments = clusterKey.split(":");
		if (segments.length > 3) {
			throw new ValidationException("ClusterKey:[" + clusterKey + "] is illegal.");
		}
	}

	private void validateGeneralProperties(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_TABLE_NAME, false, 1);
		properties.validateString(CONNECTOR_ROW_KEY, false, 1);
		properties.validateString(CONNECTOR_CF_QUALIFIER_DELIMITER, true, 1);
	}

	private void validateBatchFlushProperties(DescriptorProperties properties) {
		properties.validateBoolean(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_ENABLE, true);
		properties.validateLong(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_MUTATIONS, true);
		properties.validateMemorySize(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_SIZE_MB, true, 1024 * 1024);
		properties.validateLong(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_INTERVAL_MS, true, 0);
		properties.validateInt(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_RETRIES, true, 0);
		properties.validateLong(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_TIMEOUT_MS, true, 0);
	}
}
