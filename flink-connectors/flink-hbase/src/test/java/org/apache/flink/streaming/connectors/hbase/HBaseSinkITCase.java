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

import org.apache.flink.addons.hbase.HBaseTestingClusterAutostarter;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.connectors.hbase.util.HBaseUtils;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * IT cases for all HBase sinks.
 */
public class HBaseSinkITCase extends HBaseTestingClusterAutostarter {

	private static final String TEST_TABLE = "testTable";

	private static final ArrayList<Tuple3<String, Integer, Integer>> collection = new ArrayList<>(20);
	private static final ArrayList<TestPojo> pojoCollection = new ArrayList<>(20);
	private static final ArrayList<Row> rowCollection = new ArrayList<>(20);
	private static final List<scala.Tuple3<String, Integer, Integer>> scalaTupleCollection = new ArrayList<>(20);

	private static HTable table;

	static {
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple3<>(String.valueOf(i), i, 0));
			pojoCollection.add(new TestPojo(String.valueOf(i), i, 0));
			rowCollection.add(Row.of(String.valueOf(i), i, 0));
			scalaTupleCollection.add(new scala.Tuple3<>(String.valueOf(i), i, 0));
		}
	}

	private static final String FAMILY1 = "family1";
	private static final String F1COL1 = "col1";

	private static final String FAMILY2 = "family2";
	private static final String F2COL1 = "col1";

	private static final int rowKeyIndex = 0;
	private static final String[] columnFamilies = new String[] {null, FAMILY1, FAMILY2};
	private static final String[] qualifiers = new String[] {null, F1COL1, F2COL1};
	private static final TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {Types.STRING, Types.INT, Types.INT};

	@BeforeClass public static void activateHBaseCluster() throws Exception {
		registerHBaseMiniClusterInClasspath();
		table = prepareTable();
	}

	private static HTable prepareTable() throws IOException {
		// create a table
		TableName tableName = TableName.valueOf(TEST_TABLE);
		// column families
		byte[][] families = new byte[][]{
			Bytes.toBytes(FAMILY1),
			Bytes.toBytes(FAMILY2),
		};
		// split keys
		byte[][] splitKeys = new byte[][]{ Bytes.toBytes(4) };

		createTable(tableName, families, splitKeys);

		// get the HTable instance
		HTable table = openTable(tableName);
		return table;
	}

	// ####### HBaseSink tests ############

	@Test
	public void testHBaseTupleSink() throws Exception {
		TupleTypeInfo<Tuple3<String, Integer, Integer>> typeInfo = new TupleTypeInfo<>(Types.STRING, Types.INT, Types.INT);
		String[] tupleFieldNames = new String[] {"f0", "f1", "f2"};
		HBaseTupleSinkFunction<Tuple3<String, Integer, Integer>> sink =
			new HBaseTupleSinkFunction<>(table, new HashMap<>(), rowKeyIndex, tupleFieldNames, columnFamilies, qualifiers, fieldTypes,
				typeInfo);

		ArrayList<byte[]> rowKeys = new ArrayList<>();
		Map<String, Tuple3<String, Integer, Integer>> validationMap = new HashMap<>();

		Configuration configuration = new Configuration();
		sink.open(configuration);

		for (Tuple3<String, Integer, Integer> value : collection) {
			sink.invoke(value, SinkContextUtil.forTimestamp(0));
			rowKeys.add(Bytes.toBytes(value.f0));
			validationMap.put(value.f0, Tuple3.of(value.f0, value.f1, value.f2));
		}
		sink.close();
		validate(rowKeys, validationMap, (tuple3, i) -> tuple3.getField(i));
	}

	@Test
	public void testHBasePojoSink() throws Exception {
		PojoTypeInfo<TestPojo> typeInfo = (PojoTypeInfo<TestPojo>) Types.POJO(TestPojo.class);
		final String[] pojoFieldNames = new String[] {"key", "value", "oldValue"};
		HBasePojoSinkFunction<TestPojo> sink =
			new HBasePojoSinkFunction<>(table, new HashMap<>(), rowKeyIndex, pojoFieldNames, columnFamilies, qualifiers,
				fieldTypes, typeInfo);

		ArrayList<byte[]> rowKeys = new ArrayList<>();
		Map<String, TestPojo> validationMap = new HashMap<>();

		Configuration configuration = new Configuration();
		sink.open(configuration);

		for (TestPojo value : pojoCollection) {
			sink.invoke(value, SinkContextUtil.forTimestamp(0));
			rowKeys.add(Bytes.toBytes(value.getKey()));
			validationMap.put(value.getKey(), value);
		}
		sink.close();
		validate(rowKeys, validationMap, (pojo, i) -> {
			try {
				Field field = pojo.getClass().getDeclaredField(pojoFieldNames[i]);
				field.setAccessible(true);
				return field.get(pojo);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Test
	public void testHBaseScalaProductSink() throws Exception {
		String[] caseClassFieldNames = new String[] {"_1", "_2", "_3"};
		Class<scala.Tuple3<String, Integer, Integer>> c =
			(Class<scala.Tuple3<String, Integer, Integer>>) new scala.Tuple3<>("hello", 1, 1).getClass();
		Seq<TypeInformation<?>> typeInfos = JavaConverters.asScalaBufferConverter(Arrays.asList(fieldTypes)).asScala();
		Seq<String> fieldNames = JavaConverters.asScalaBufferConverter(Arrays.asList(caseClassFieldNames)).asScala();

		CaseClassTypeInfo<scala.Tuple3<String, Integer, Integer>> typeInfo =
			new CaseClassTypeInfo<scala.Tuple3<String, Integer, Integer>>(c, null, typeInfos, fieldNames) {
				@Override public TypeSerializer<scala.Tuple3<String, Integer, Integer>> createSerializer(
					ExecutionConfig config) {
					return null;
				}
			};

		HBaseScalaProductSinkFunction<scala.Tuple3<String, Integer, Integer>> sink =
			new HBaseScalaProductSinkFunction<>(table, new HashMap<>(), rowKeyIndex, caseClassFieldNames, columnFamilies, qualifiers, fieldTypes, typeInfo);

		ArrayList<byte[]> rowKeys = new ArrayList<>();
		Map<String, scala.Tuple3<String, Integer, Integer>> validationMap = new HashMap<>();

		for (scala.Tuple3<String, Integer, Integer> value : scalaTupleCollection) {
			sink.invoke(value, SinkContextUtil.forTimestamp(0));
			rowKeys.add(Bytes.toBytes(value._1()));
			validationMap.put(value._1(), value);
		}
		sink.close();
		validate(rowKeys, validationMap, (tuple, i) -> tuple.productElement(i));
	}

	@Test
	public void testHBaseRowSink() throws Exception {
		String[] rowFieldNames = new String[] {"key", "value", "oldValue"};
		RowTypeInfo typeInfo = (RowTypeInfo) Types.ROW_NAMED(rowFieldNames, fieldTypes);
		HBaseRowSinkFunction<Row> sink = new HBaseRowSinkFunction(table, new HashMap<>(), rowKeyIndex, rowFieldNames, columnFamilies,
			qualifiers, fieldTypes, typeInfo);

		ArrayList<byte[]> rowKeys = new ArrayList<>();
		Map<String, Row> validationMap = new HashMap<>();

		for (Row value : rowCollection) {
			sink.invoke(value, SinkContextUtil.forTimestamp(0));
			String valueKey = (String) value.getField(rowKeyIndex);
			rowKeys.add(Bytes.toBytes(valueKey));
			validationMap.put(valueKey, value);
		}
		validate(rowKeys, validationMap, (row, i) -> row.getField(i));
	}

	@Test
	public void testHBaseUpsertSink() throws Exception {
		String[] rowFieldNames = new String[] {"key", "value", "oldValue"};
		RowTypeInfo typeInfo = (RowTypeInfo) Types.ROW_NAMED(rowFieldNames, fieldTypes);
		HBaseUpsertTableSink.HBaseUpsertSinkFunction sink =
			new HBaseUpsertTableSink.HBaseUpsertSinkFunction(table, new HashMap<>(), rowKeyIndex, rowFieldNames, columnFamilies,
				qualifiers, fieldTypes, typeInfo);

		ArrayList<byte[]> rowKeys = new ArrayList<>();
		Map<String, Row> validationMap = new HashMap<>();

		for (Row value : rowCollection) {
			sink.invoke(Tuple2.of(true, value), SinkContextUtil.forTimestamp(0));
			String valueKey = (String) value.getField(rowKeyIndex);
			rowKeys.add(Bytes.toBytes(valueKey));
			validationMap.put(valueKey, value);
		}
		validate(rowKeys, validationMap, (row, i) -> row.getField(i));

		for (Row value : rowCollection) {
			sink.invoke(Tuple2.of(false, value), SinkContextUtil.forTimestamp(0));
			String valueKey = (String) value.getField(rowKeyIndex);
			rowKeys.add(Bytes.toBytes(valueKey));
			validationMap.put(valueKey, value);
		}
		validateNull(rowKeys);
	}

	private <T> void validate(ArrayList<byte[]> rowKeys, Map<String, T> validationMap, BiFunction<T, Integer, Object> biFunction)
		throws IOException {
		for (byte[] rowKey : rowKeys) {
			for (int i = 0; i < fieldTypes.length; i++) {
				if (i != rowKeyIndex) {
					byte[] result = table.get(new Get(rowKey)).getValue(columnFamilies[i].getBytes(), qualifiers[i].getBytes());
					byte[] validation = HBaseUtils.serialize(fieldTypes[i], biFunction.apply(validationMap.get(new String(rowKey)), i));
					Assert.assertTrue(Arrays.equals(result, validation));
				}
			}
		}
	}

	private void validateNull(ArrayList<byte[]> rowKeys) throws IOException {
		for (byte[] rowKey : rowKeys) {
			for (int i = 0; i < fieldTypes.length; i++) {
				if (i != rowKeyIndex) {
					byte[] result = table.get(new Get(rowKey)).getValue(columnFamilies[i].getBytes(), qualifiers[i].getBytes());
					Assert.assertNull(result);
				}
			}
		}
	}

//	@Test
//	public void testHBaseRowSink() throws Exception {
////		HBaseTableMapper tableMapper = new HBaseTableMapper();
////		tableMapper.addMapping("key", FAMILY1, F1COL1, String.class)
////			.addMapping("value", FAMILY2, F2COL1, Integer.class)
////			.addMapping("oldValue", FAMILY2, F2COL2, Integer.class)
////			.setRowKey("key", String.class);
////
////		ArrayList<byte[]> rowKeys = new ArrayList<>();
////		String[] keyList = tableMapper.getKeyList();
////		Map<String, Row> validationMap = new HashMap<>();
////
////		TypeInformation[] types = {Types.STRING(), Types.INT(), Types.INT()};
////		String[] fieldNames = {"key", "value", "oldValue"};
////		RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);
////
////		HBaseRowSinkFunction sink = new HBaseRowSinkFunction(table, tableMapper, rowTypeInfo);
////
////		Configuration configuration = new Configuration();
////		sink.open(configuration);
////
////		for (Row value : rowCollection) {
////			sink.invoke(value, SinkContextUtil.forTimestamp(0));
////			String valueKey = (String) value.getField(rowTypeInfo.getFieldIndex("key"));
////			rowKeys.add(Bytes.toBytes(valueKey));
////			validationMap.put(valueKey, value);
////		}
////		sink.close();
////
////		for (byte[] rowKey : rowKeys) {
////			for (int i = 0; i < keyList.length; i++) {
////				HBaseColumnInfo colInfo = tableMapper.getColInfo(keyList[i]);
////				byte[] result = table.get(new Get(rowKey)).getValue(colInfo.getColumnFamily().getBytes(), colInfo.getQualifier().getBytes());
////				byte[] validation = HBaseTableMapper.serialize(colInfo.getTypeInfo(),
////					validationMap.get(new String(rowKey, tableMapper.getCharset()))
////						.getField(rowTypeInfo.getFieldIndex(keyList[i])));
////				Assert.assertTrue(Arrays.equals(result, validation));
////			}
////		}
//	}
}
