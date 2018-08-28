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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import scala.Product;

/**
 * This class wraps different HBase sink implementations to provide a common interface for all of them.
 *
 * @param <IN> input type
 */
public class HBaseSink<IN> {

	private DataStreamSink<IN> sink;

	private HBaseSink(DataStreamSink<IN> sink) {
		this.sink = sink;
	}

	private SinkTransformation<IN> getSinkTransformation() {
		return sink.getTransformation();
	}

	/**
	 * Sets the name of this sink. This name is
	 * used by the visualization and logging during runtime.
	 *
	 * @return The named sink.
	 */
	public HBaseSink<IN> name(String name) {
		getSinkTransformation().setName(name);
		return this;
	}

	/**
	 * Sets an ID for this operator.
	 *
	 * <p>The specified ID is used to assign the same operator ID across job
	 * submissions (for example when starting a job from a savepoint).
	 *
	 * <p><strong>Important</strong>: this ID needs to be unique per
	 * transformation and job. Otherwise, job submission will fail.
	 *
	 * @param uid The unique user-specified ID of this transformation.
	 * @return The operator with the specified ID.
	 */
	@PublicEvolving
	public HBaseSink<IN> uid(String uid) {
		getSinkTransformation().setUid(uid);
		return this;
	}

	/**
	 * Sets an user provided hash for this operator. This will be used AS IS the create the JobVertexID.
	 *
	 * <p>The user provided hash is an alternative to the generated hashes, that is considered when identifying an
	 * operator through the default hash mechanics fails (e.g. because of changes between Flink versions).
	 *
	 * <p><strong>Important</strong>: this should be used as a workaround or for trouble shooting. The provided hash
	 * needs to be unique per transformation and job. Otherwise, job submission will fail. Furthermore, you cannot
	 * assign user-specified hash to intermediate nodes in an operator chain and trying so will let your job fail.
	 *
	 * <p>A use case for this is in migration between Flink versions or changing the jobs in a way that changes the
	 * automatically generated hashes. In this case, providing the previous hashes directly through this method (e.g.
	 * obtained from old logs) can help to reestablish a lost mapping from states to their target operator.
	 *
	 * @param uidHash The user provided hash for this operator. This will become the JobVertexID, which is shown in the
	 *                 logs and web ui.
	 * @return The operator with the user provided hash.
	 */
	@PublicEvolving
	public HBaseSink<IN> setUidHash(String uidHash) {
		getSinkTransformation().setUidHash(uidHash);
		return this;
	}

	/**
	 * Sets the parallelism for this sink. The degree must be higher than zero.
	 *
	 * @param parallelism The parallelism for this sink.
	 * @return The sink with set parallelism.
	 */
	public HBaseSink<IN> setParallelism(int parallelism) {
		getSinkTransformation().setParallelism(parallelism);
		return this;
	}

	/**
	 * Turns off chaining for this operator so thread co-location will not be
	 * used as an optimization.
	 * <p/>
	 * <p/>
	 * Chaining can be turned off for the whole
	 * job by {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#disableOperatorChaining()}
	 * however it is not advised for performance considerations.
	 *
	 * @return The sink with chaining disabled
	 */
	public HBaseSink<IN> disableChaining() {
		getSinkTransformation().setChainingStrategy(ChainingStrategy.NEVER);
		return this;
	}

	/**
	 * Sets the slot sharing group of this operation. Parallel instances of
	 * operations that are in the same slot sharing group will be co-located in the same
	 * TaskManager slot, if possible.
	 *
	 * <p>Operations inherit the slot sharing group of input operations if all input operations
	 * are in the same slot sharing group and no slot sharing group was explicitly specified.
	 *
	 * <p>Initially an operation is in the default slot sharing group. An operation can be put into
	 * the default group explicitly by setting the slot sharing group to {@code "default"}.
	 *
	 * @param slotSharingGroup The slot sharing group name.
	 */
	public HBaseSink<IN> slotSharingGroup(String slotSharingGroup) {
		getSinkTransformation().setSlotSharingGroup(slotSharingGroup);
		return this;
	}

	/**
	 * Writes a DataStream into a HBase database.
	 *
	 * @param input input DataStream
	 * @param <IN>  input type
	 * @return HBaseSinkBuilder, to further configure the sink
	 */
	public static <IN> HBaseSinkBuilder<IN> addSink(org.apache.flink.streaming.api.scala.DataStream<IN> input) {
		return addSink(input.javaStream());
	}

	/**
	 * Writes a DataStream into a HBase database.
	 *
	 * @param input input DataStream
	 * @param <IN>  input type
	 * @return HBaseSinkBuilder, to further configure the sink
	 */
	public static <IN> HBaseSinkBuilder<IN> addSink(DataStream<IN> input) {
		TypeInformation<IN> typeInfo = input.getType();
		if (typeInfo instanceof TupleTypeInfo) {
			DataStream<Tuple> tupleInput = (DataStream<Tuple>) input;
			return (HBaseSinkBuilder<IN>) new HBaseTupleSinkBuilder<>(tupleInput, tupleInput.getType(), tupleInput.getType().createSerializer(tupleInput.getExecutionEnvironment().getConfig()));
		}
		if (typeInfo instanceof PojoTypeInfo) {
			return new HBasePojoSinkBuilder<>(input, input.getType(), input.getType().createSerializer(input.getExecutionEnvironment().getConfig()));
		}
		if (typeInfo instanceof CaseClassTypeInfo) {
			DataStream<Product> productInput = (DataStream<Product>) input;
			return (HBaseSinkBuilder<IN>) new HBaseScalaProductSinkBuilder<>(productInput, productInput.getType(), productInput.getType().createSerializer(input.getExecutionEnvironment().getConfig()));
		}
		if (typeInfo instanceof RowTypeInfo) {
			DataStream<Row> rowInput = (DataStream<Row>) input;
			return (HBaseSinkBuilder<IN>) new HBaseRowSinkBuilder<>(rowInput, rowInput.getType(), rowInput.getType().createSerializer(input.getExecutionEnvironment().getConfig()));
		}
		throw new IllegalArgumentException("No support for the type of the given DataStream: " + input.getType());
	}

	/**
	 * Builder for a {@link HBaseSink}.
	 * @param <IN>
	 */
	public abstract static class HBaseSinkBuilder<IN> {
		protected final DataStream<IN> input;
		protected final TypeSerializer<IN> serializer;
		protected final TypeInformation<IN> typeInfo;

		protected String clusterKey;
		protected String tableName;
		protected int rowKeyIndex = -1;
		protected final List<String> fieldNames;
		protected final List<String> columnFamilies;
		protected final List<String> qualifiers;
		protected final List<TypeInformation<?>> fieldTypes;

		protected final Map<String, String> userConfig;

		public HBaseSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			this.input = input;
			this.serializer = serializer;
			this.typeInfo = typeInfo;
			this.fieldNames = new LinkedList<>();
			this.columnFamilies = new LinkedList<>();
			this.qualifiers = new LinkedList<>();
			this.fieldTypes = new LinkedList<>();
			this.userConfig = new HashMap<>();
		}

		/**
		 * Sets the cluster key of HBase to connect to.
		 * @param clusterKey
		 * @return this builder
		 * @throws IOException
		 */
		public HBaseSinkBuilder<IN> setClusterKey(String clusterKey) throws IOException {
			this.clusterKey = clusterKey;
			return this;
		}

		/**
		 * Sets the name of table to be used.
		 * @param tableName
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> setTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		/**
		 * Sets the field used as row key.
		 * @param rowKeyField
		 * @param rowKeyType
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> setRowKeyField(String rowKeyField, TypeInformation<?> rowKeyType) {
			fieldNames.add(rowKeyField);
			columnFamilies.add("");
			qualifiers.add("");
			fieldTypes.add(rowKeyType);
			rowKeyIndex = fieldNames.size() - 1;
			return this;
		}

		/**
		 * Sets the field used as row key.
		 * @param rowKeyIndex
		 * @param rowKeyType
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> setRowKeyField(int rowKeyIndex, TypeInformation<?> rowKeyType) {
			fieldNames.add("f" + rowKeyIndex);
			columnFamilies.add("");
			qualifiers.add("");
			fieldTypes.add(rowKeyType);
			rowKeyIndex = fieldNames.size() - 1;
			return this;
		}

		/**
		 * Adds a field with related column family, qualifier and type information.
		 * This is for POJO sink.
		 * @param fieldName
		 * @param columnFamily
		 * @param qualifier
		 * @param typeInfo
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> addField(String fieldName, String columnFamily, String qualifier, TypeInformation<?> typeInfo) {
			fieldNames.add(fieldName);
			columnFamilies.add(columnFamily);
			qualifiers.add(qualifier);
			fieldTypes.add(typeInfo);
			return this;
		}

		/**
		 * Adds a field index with related column family, qualifier and type information.
		 * This is for Tuple sink.
		 * @param fieldIndex
		 * @param columnFamily
		 * @param qualifier
		 * @param typeInfo
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> addField(int fieldIndex, String columnFamily, String qualifier, TypeInformation<?> typeInfo) {
			fieldNames.add("f" + fieldIndex);
			columnFamilies.add(columnFamily);
			qualifiers.add(qualifier);
			fieldTypes.add(typeInfo);
			return this;
		}

		/**
		 * Adds all fields' column families, qualifiers and type informations.
		 * This is for Row sink.
		 * @param fieldNames
		 * @param columnFamilies
		 * @param qualifiers
		 * @param typeInfos
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> addFields(String[] fieldNames, String[] columnFamilies, String[] qualifiers, TypeInformation<?>[] typeInfos) {
			this.fieldNames.addAll(Arrays.asList(fieldNames));
			this.columnFamilies.addAll(Arrays.asList(columnFamilies));
			this.qualifiers.addAll(Arrays.asList(qualifiers));
			this.fieldTypes.addAll(Arrays.asList(typeInfos));
			return this;
		}

		/**
		 * Enable the client buffer for HBase.
		 * Only flush when buffer is full or during checkpoint.
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> enableBuffer() {
			userConfig.put(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_ENABLE, "true");
			return this;
		}

		/**
		 * Disable the client buffer for HBase.
		 * Flush to HBase on every operation. This might decrease the throughput and increase latency.
		 * @return this builder
		 */
		public HBaseSinkBuilder<IN> disableBuffer() {
			userConfig.put(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_ENABLE, "false");
			return this;
		}

		/**
		 * Set the auto flush period of buffer for HBase.
		 * @param seconds flush period
		 * @return
		 */
		public HBaseSinkBuilder<IN> setFlushInterval(int seconds) {
			long flushInvervalInMillis = seconds * 1000;
			userConfig.put(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_INTERVAL_MS, String.valueOf(flushInvervalInMillis));
			return this;
		}

		/**
		 * Set buffer size for HBase.
		 * @param bufferSizeMb
		 * @return
		 */
		public HBaseSinkBuilder<IN> setBufferSize(long bufferSizeMb) {
			userConfig.put(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_MAX_SIZE_MB, String.valueOf(bufferSizeMb));
			return this;
		}

		/**
		 * Finalizes the configuration of this sink.
		 *
		 * @return finalized sink
		 * @throws Exception
		 */
		public HBaseSink<IN> build() throws Exception {
			return createSink();
		}

		protected String[] getFieldNamesArray() {
			return fieldNames.toArray(new String[fieldNames.size()]);
		}

		protected String[] getColumnFamiliesArray() {
			return columnFamilies.toArray(new String[columnFamilies.size()]);
		}

		protected String[] getQualifiersArray() {
			return qualifiers.toArray(new String[qualifiers.size()]);
		}

		protected TypeInformation<?>[] getFieldTypesArray() {
			return fieldTypes.toArray(new TypeInformation<?>[fieldTypes.size()]);
		}

		protected abstract HBaseSink<IN> createSink() throws Exception;

		protected void sanityCheck() {
			if (StringUtils.isEmpty(clusterKey)) {
				throw new IllegalArgumentException("HBase cluster key must be supplied using setClusterKey().");
			}
			if (StringUtils.isEmpty(tableName)) {
				throw new IllegalArgumentException("HBase table name must be supplied using setTableName().");
			}
			if (rowKeyIndex < 0) {
				throw new IllegalArgumentException("Rowkey must be supplied using setRowKeyField().");
			}
			if (fieldNames.size() < 2) {
				throw new IllegalArgumentException("At least two field should be set for HBase Sinks. One for rowkey and one for column");
			}
			for (String fieldName : fieldNames) {
				if (StringUtils.isEmpty(fieldName)) {
					throw new IllegalArgumentException("The fieldNames for sink cannot be empty.");
				}
			}
		}
	}

	/**
	 * Builder for a {@link HBaseTupleSinkFunction}.
	 * @param <IN>
	 */
	public static class HBaseTupleSinkBuilder<IN extends Tuple> extends HBaseSinkBuilder<IN> {

		public HBaseTupleSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			super(input, typeInfo, serializer);
		}

		@Override
		protected void sanityCheck() {
			super.sanityCheck();
			for (String key : fieldNames) {
				if (key.charAt(0) != 'f' || StringUtils.isNumeric(key.substring(1, key.length()))) {
					throw new IllegalArgumentException("Illegal key for tuples, the key must start with 'f' and followed by a number.");
				}
			}
		}

		@Override public HBaseSinkBuilder<IN> addField(String fieldName, String columnFamily, String qualifier,
			TypeInformation<?> typeInfo) {
			throw new RuntimeException("Fields of HBase tuple sink cannot be set with String format field name");
		}

		@Override
		protected HBaseSink<IN> createSink() throws Exception {
			return new HBaseSink<>(input.addSink(new HBaseTupleSinkFunction<IN>(
				clusterKey,
				tableName,
				userConfig,
				rowKeyIndex,
				getFieldNamesArray(),
				getColumnFamiliesArray(),
				getQualifiersArray(),
				getFieldTypesArray(),
				(TupleTypeInfo<IN>) typeInfo)));
		}
	}

	/**
	 * Builder for a {@link HBasePojoSinkFunction}.
	 * @param <IN>
	 */
	public static class HBasePojoSinkBuilder<IN> extends HBaseSinkBuilder<IN> {

		public HBasePojoSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			super(input, typeInfo, serializer);
		}

		@Override public HBaseSinkBuilder<IN> setRowKeyField(int rowKeyIndex, TypeInformation<?> rowKeyType) {
			throw new RuntimeException("Fields of HBase tuple sink cannot be set with index of field name");
		}

		@Override protected HBaseSink<IN> createSink() throws Exception {
			return new HBaseSink<>(input.addSink(new HBasePojoSinkFunction<IN>(
				clusterKey,
				tableName,
				userConfig,
				rowKeyIndex,
				getFieldNamesArray(),
				getColumnFamiliesArray(),
				getQualifiersArray(),
				getFieldTypesArray(),
				(PojoTypeInfo<IN>) typeInfo)));
		}
	}

	/**
	 * Builder for a {@link HBaseScalaProductSinkFunction}.
	 * @param <IN>
	 */
	public static class HBaseScalaProductSinkBuilder<IN extends Product> extends HBaseSinkBuilder<IN> {

		public HBaseScalaProductSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			super(input, typeInfo, serializer);
		}

		@Override protected HBaseSink<IN> createSink() throws Exception {
			return new HBaseSink<>(input.addSink(new HBaseScalaProductSinkFunction<IN>(
				clusterKey,
				tableName,
				userConfig,
				rowKeyIndex,
				getFieldNamesArray(),
				getColumnFamiliesArray(),
				getQualifiersArray(),
				getFieldTypesArray(),
				(CaseClassTypeInfo<IN>) typeInfo)));
		}

		@Override public HBaseSinkBuilder<IN> setRowKeyField(int rowKeyIndex, TypeInformation<?> rowKeyType) {
			throw new RuntimeException("Fields of HBase tuple sink cannot be set with index of field name");
		}

		@Override
		protected void sanityCheck() {
			super.sanityCheck();
			for (String key : fieldNames) {
				if (StringUtils.isNumeric(key)) {
					throw new IllegalArgumentException("The key: " + key + " for scala product sink must be index of product element.");
				}
			}
		}
	}

	/**
	 * Builder for a {@link HBaseRowSinkFunction}.
	 * @param <IN>
	 */
	public static class HBaseRowSinkBuilder<IN extends Row> extends HBaseSinkBuilder<IN> {

		public HBaseRowSinkBuilder(DataStream<IN> input, TypeInformation<IN> typeInfo, TypeSerializer<IN> serializer) {
			super(input, typeInfo, serializer);
		}

		@Override public HBaseSinkBuilder<IN> setRowKeyField(int rowKeyIndex, TypeInformation<?> rowKeyType) {
			throw new RuntimeException("Fields of HBase tuple sink cannot be set with index of field name");
		}

		@Override protected HBaseSink<IN> createSink() throws Exception {
			return new HBaseSink<>(input.addSink(new HBaseRowSinkFunction<IN>(
				clusterKey,
				tableName,
				userConfig,
				rowKeyIndex,
				getFieldNamesArray(),
				getColumnFamiliesArray(),
				getQualifiersArray(),
				getFieldTypesArray(),
				(RowTypeInfo) typeInfo)));
		}
	}
}
