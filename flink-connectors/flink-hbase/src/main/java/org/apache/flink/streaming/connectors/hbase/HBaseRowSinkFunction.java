/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;

import java.util.Map;

/**
 * Sink to write flink Rows into a HBase cluster.
 */
public class HBaseRowSinkFunction<IN extends Row> extends HBaseSinkFunctionBase<IN> {

	public HBaseRowSinkFunction(
		String clusterKey,
		String tableName,
		Map<String, String> userConfig,
		int rowKeyIndex,
		String[] fieldNames,
		String[] columnFamilies,
		String[] qualifiers,
		TypeInformation<?>[] fieldTypes,
		RowTypeInfo typeInfo) {
		super(clusterKey, tableName, userConfig, rowKeyIndex, typeInfo.getFieldNames(), fieldNames, columnFamilies, qualifiers,	fieldTypes);
	}

	@VisibleForTesting
	public HBaseRowSinkFunction(
		Table table,
		Map<String, String> userConfig,
		int rowKeyIndex,
		String[] fieldNames,
		String[] columnFamilies,
		String[] qualifiers,
		TypeInformation<?>[] fieldTypes,
		RowTypeInfo typeInfo) {
		super(table, userConfig, rowKeyIndex, typeInfo.getFieldNames(), fieldNames, columnFamilies, qualifiers,	fieldTypes);
	}

	@Override protected Mutation extract(IN value) {
		return generatePutMutation(value);
	}

	@Override protected Object produceElementWithIndex(IN value, int index) {
		return value.getField(fieldElementIndexMapping[index]);
	}
}
