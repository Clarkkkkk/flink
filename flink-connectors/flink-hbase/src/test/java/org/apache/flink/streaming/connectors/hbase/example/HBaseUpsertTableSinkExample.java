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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.HBase;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.UUID;

/**
 * This is an example showing the to use the Upsert HBase Table Sink.
 *
 * <p>The example assumes that a table exists in a local hbase database.
 */
public class HBaseUpsertTableSinkExample {
	private static final ArrayList<Row> messages = new ArrayList<>(20);
	private static final String TEST_TABLE = "testTable";

	static {
		for (int i = 0; i < 20; i++) {
			messages.add(Row.of(UUID.randomUUID().toString(), i, i * 10));
		}
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);

		DataStreamSource<Row> source = env.fromCollection(messages);
		tableEnvironment.registerDataStream("testSource", source);

		tableEnvironment
			.connect(new HBase().clusterKey("127.0.0.1:2181:/hbase").tableName(TEST_TABLE).rowKey("rowkey").delimiter(":").enableBatchFlush())
			.withSchema(new Schema().field("rowkey", Types.STRING).field("cf:qualifier", Types.INT).field("cf:qualifier1", Types.INT))
			.inUpsertMode()
			.registerTableSink("testSink");
		tableEnvironment.sqlUpdate("insert into testSink select f0 as rowkey, f1 as `cf:qualifier`, f2 as `cf:qualifier1` from testSource");
//		tableEnvironment.sqlUpdate("insert into testSink select f0 as rowkey, sum(f1) as `cf:qualifier`, sum(f2) as `cf:qualifier1` from testSource group by f0, f1, f2");
		env.execute();

	}
}
