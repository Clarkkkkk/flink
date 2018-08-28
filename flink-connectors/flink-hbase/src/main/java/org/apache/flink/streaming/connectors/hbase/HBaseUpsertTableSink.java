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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;

import java.util.Arrays;
import java.util.Map;

/**
 * Upsert table sink for HBase.
 */
public class HBaseUpsertTableSink implements UpsertStreamTableSink<Row> {

	/** Flag that indicates that only inserts are accepted. */
	private final boolean isAppendOnly;

	/** Schema of the table. */
	private final TableSchema schema;

	private final String clusterKey;

	private final String tableName;

	private final Map<String, String> userConfig;

	/** The rowkey field of each row. */
	private final String rowKeyField;

	private final String delimiter;

	/** Key field indices determined by the query. */
	private int[] keyFieldIndices = new int[0];

	public HBaseUpsertTableSink(boolean isAppendOnly, TableSchema schema, String clusterKey, String tableName,
		Map<String, String> userConfig, String rowKeyField, String delimiter) {
		this.isAppendOnly = isAppendOnly;
		this.schema = schema;
		this.clusterKey = clusterKey;
		this.tableName = tableName;
		this.userConfig = userConfig;
		this.rowKeyField = rowKeyField;
		this.delimiter = delimiter;
	}

	@Override public void setKeyFields(String[] keyNames) {
		// HBase update rely on rowkey
	}

	@Override public void setIsAppendOnly(Boolean isAppendOnly) {
		if (this.isAppendOnly && !isAppendOnly) {
			throw new ValidationException(
				"The given query is not supported by this sink because the sink is configured to " +
					"operate in append mode only. Thus, it only support insertions (no queries " +
					"with updating results).");
		}
	}

	@Override public TypeInformation<Row> getRecordType() {
		return schema.toRowType();
	}

	@Override public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		String[] fieldNames = getFieldNames();
		String[] columnFamilies = new String[fieldNames.length];
		String[] qualifiers = new String[fieldNames.length];
		int rowKeyIndex = -1;
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(rowKeyField)) {
				rowKeyIndex = i;
			} else {
				String[] split = fieldNames[i].split(delimiter);
				if (split.length != 2) {
					throw new RuntimeException("Column family and qualifer cannot be derived with field " + fieldNames[i]
						+ " and delimiter " + delimiter + ".");
				}
				columnFamilies[i] = split[0];
				qualifiers[i] = split[1];
			}
		}
		if (rowKeyIndex == -1) {
			throw new RuntimeException("Row key field " + rowKeyField + " cannot be found.");
		}

		final HBaseUpsertSinkFunction upsertSinkFunction =
			new HBaseUpsertSinkFunction(
				clusterKey,
				tableName,
				userConfig,
				rowKeyIndex,
				getFieldNames(),
				columnFamilies,
				qualifiers,
				getFieldTypes(),
				(RowTypeInfo) getRecordType()
			);

		dataStream.addSink(upsertSinkFunction)
			.name(TableConnectorUtil.generateRuntimeName(this.getClass(), getFieldNames()));
	}

	@Override public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
		return Types.TUPLE(Types.BOOLEAN, getRecordType());
	}

	@Override public String[] getFieldNames() {
		return schema.getFieldNames();
	}

	@Override public TypeInformation<?>[] getFieldTypes() {
		return schema.getFieldTypes();
	}

	@Override public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
			throw new ValidationException("Reconfiguration with different fields is not allowed. " +
				"Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
				"But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
		}
		return new HBaseUpsertTableSink(isAppendOnly, schema, clusterKey, tableName, userConfig, rowKeyField, delimiter);
	}

	/**
	 * Sink to write row values into a HBase cluster in upsert mode.
	 */
	public static class HBaseUpsertSinkFunction extends HBaseSinkFunctionBase<Tuple2<Boolean, Row>> {

		public HBaseUpsertSinkFunction(
			String clusterKey,
			String tableName,
			Map<String, String> userConfig,
			int rowKeyIndex,
			String[] fieldNames,
			String[] columnFamilies,
			String[] qualifiers,
			TypeInformation<?>[] fieldTypes,
			RowTypeInfo typeInfo) {
			super(clusterKey, tableName, userConfig, rowKeyIndex, typeInfo.getFieldNames(), fieldNames, columnFamilies, qualifiers, fieldTypes);
		}

		@VisibleForTesting
		public HBaseUpsertSinkFunction(
			Table table,
			Map<String, String> userConfig,
			int rowKeyIndex,
			String[] fieldNames,
			String[] columnFamilies,
			String[] qualifiers,
			TypeInformation<?>[] fieldTypes,
			RowTypeInfo typeInfo) {
			super(table, userConfig, rowKeyIndex, typeInfo.getFieldNames(), fieldNames, columnFamilies, qualifiers, fieldTypes);
		}

		@Override protected Mutation extract(Tuple2<Boolean, Row> value) {
			Row row = value.f1;
			if (value.f0) {
				return generatePutMutation(value);
			} else {
				return generateDeleteMutation(value);
			}
		}

		@Override protected Object produceElementWithIndex(Tuple2<Boolean, Row> value, int index) {
			return value.f1.getField(fieldElementIndexMapping[index]);
		}
	}
}
