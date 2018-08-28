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

package org.apache.flink.streaming.connectors.hbase.example;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.hbase.HBaseSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.UUID;

/**
 * This is an example showing the to use the Row HBase Sink in the Streaming API.
 *
 * <p>The example assumes that a table exists in a local hbase database.
 */
public class HBaseRowSinkExample {
	private static final ArrayList<Row> messages = new ArrayList<>(20);
	private static final String TEST_TABLE = "testTable";

	static {
		for (int i = 0; i < 20; i++) {
			messages.add(Row.of(UUID.randomUUID().toString(), i, i * 10));
		}
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		TypeInformation[] types = {Types.STRING, Types.INT, Types.INT};

		String[] fields = {"key", "value", "oldValue"};

		DataStreamSource<Row> source = env.fromCollection(messages, new RowTypeInfo(types, fields));

		HBaseSink.addSink(source)
			.setClusterKey("127.0.0.1:2181:/hbase")
			.setTableName(TEST_TABLE)
			.addField("value", "CF1", "QUALIFER1", Types.INT)
			.addField("oldValue", "CF2", "QUALIFIER2", Types.INT)
			.setRowKeyField("key", Types.STRING)
			.build();
//		HBaseSink.addSink(source)
//			.setClusterKey("127.0.0.1:2181:/hbase")
//			.setTableMapper(
//				new HBaseTableMapper()
//					.addMapping("key", "CF1", "key", String.class)
//					.addMapping("value", "CF2", "value", Integer.class)
//					.addMapping("oldValue", "CF2", "oldValue", Integer.class)
//					.setRowKey("key", String.class)
//			)
//			.setTableName(TEST_TABLE)
//			.build();

		env.execute("HBase Sink example");
	}
}
