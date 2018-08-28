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
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;

import java.util.Map;

import scala.Product;

/**
 * Sink to write scala tuples and case classes into a HBase cluster.
 *
 * @param <IN> Type of the elements emitted by this sink, it must extend {@link Product}
 */
public class HBaseScalaProductSinkFunction<IN extends Product> extends HBaseSinkFunctionBase<IN> {

	public HBaseScalaProductSinkFunction(
		String clusterKey,
		String tableName,
		Map<String, String> userConfig,
		int rowKeyIndex,
		String[] fieldNames,
		String[] columnFamilies,
		String[] qualifiers,
		TypeInformation<?>[] fieldTypes,
		CaseClassTypeInfo<IN> typeInfo) {
		super(clusterKey, tableName, userConfig, rowKeyIndex, typeInfo.getFieldNames(), fieldNames, columnFamilies, qualifiers, fieldTypes);
	}

	@VisibleForTesting
	public HBaseScalaProductSinkFunction(
		Table hTable,
		Map<String, String> userConfig,
		int rowKeyIndex,
		String[] fieldNames,
		String[] columnFamilies,
		String[] qualifiers,
		TypeInformation<?>[] fieldTypes,
		CaseClassTypeInfo<IN> typeInfo) {
		super(hTable, userConfig, rowKeyIndex, typeInfo.getFieldNames(), fieldNames, columnFamilies, qualifiers, fieldTypes);
	}

	protected  Object getElement(IN value, int index) {
		return value.productElement(index);
	}

	@Override protected Mutation extract(IN value) {
		return generatePutMutation(value);
	}

	@Override protected Object produceElementWithIndex(IN value, int index) {
		return value.productElement(fieldElementIndexMapping[index]);
	}
}
