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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Sink to write POJO into a HBase cluster.
 *
 * @param <IN> Type of the element emitted by this sink, it must be a POJO
 */
public class HBasePojoSinkFunction<IN> extends HBaseSinkFunctionBase<IN> {

	private static final long serialVersionUID = 1L;

	private final PojoTypeInfo<IN> typeInfo;

	private Field[] fields;

	/**
	 * The main constructor for creating HBasePojoSinkFunction.
	 *
	 * @param typeInfo TypeInformation of the Pojo
	 */
	public HBasePojoSinkFunction(
		String clusterKey,
		String tableName,
		Map<String, String> userConfig,
		int rowKeyIndex,
		String[] fieldNames,
		String[] columnFamilies,
		String[] qualifiers,
		TypeInformation<?>[] fieldTypes,
		PojoTypeInfo<IN> typeInfo) {
		super(clusterKey, tableName, userConfig, rowKeyIndex, typeInfo.getFieldNames(), fieldNames, columnFamilies, qualifiers, fieldTypes);
		this.typeInfo = typeInfo;
	}

	public HBasePojoSinkFunction(
		Table hTable,
		Map<String, String> userConfig,
		int rowKeyIndex,
		String[] fieldNames,
		String[] columnFamilies,
		String[] qualifiers,
		TypeInformation<?>[] fieldTypes,
		PojoTypeInfo<IN> typeInfo) {
		super(hTable, userConfig, rowKeyIndex, typeInfo.getFieldNames(), fieldNames, columnFamilies, qualifiers, fieldTypes);
		this.typeInfo = typeInfo;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		fields = new Field[typeInfo.getArity()];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = typeInfo.getPojoFieldAt(i).getField();
			fields[i].setAccessible(true);
		}
	}

	@Override
	protected Mutation extract(IN value) {
		return generatePutMutation(value);
	}

	@Override protected Object produceElementWithIndex(IN value, int index) {
		try {
			return fields[fieldElementIndexMapping[index]].get(value);
		} catch (IllegalAccessException e) {
			// This should never happen.
			throw new RuntimeException("Cannot access the field.", e);
		}
	}

}
