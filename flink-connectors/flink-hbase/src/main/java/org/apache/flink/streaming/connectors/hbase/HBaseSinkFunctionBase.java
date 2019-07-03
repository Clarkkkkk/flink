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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.hbase.util.HBaseUtils;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * HBaseSinkFunctionBase is the common abstract class of {@link HBasePojoSinkFunction}, {@link HBaseTupleSinkFunction},
 * {@link HBaseScalaProductSinkFunction} and {@link HBaseUpsertTableSink.HBaseUpsertSinkFunction}.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class HBaseSinkFunctionBase<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

	private static final Logger log = LoggerFactory.getLogger(HBaseSinkFunctionBase.class);

	// ------------------------------------------------------------------------
	//  Internal bulk processor configuration
	// ------------------------------------------------------------------------

	public static final String CONFIG_KEY_BATCH_FLUSH_ENABLE = "connector.batch-flush.enable";
	public static final String CONFIG_KEY_BATCH_FLUSH_MAX_MUTATIONS = "connector.batch-flush.max-mutations";
	public static final String CONFIG_KEY_BATCH_FLUSH_MAX_SIZE_MB = "connector.batch-flush.max-size.mb";
	public static final String CONFIG_KEY_BATCH_FLUSH_INTERVAL_MS = "connector.batch-flush.interval.ms";
	public static final String CONFIG_KEY_BATCH_FLUSH_MAX_RETRIES = "connector.batch-flush.backoff.max-retries";
	public static final String CONFIG_KEY_BATCH_FLUSH_MAX_TIMEOUT_MS = "connector.batch-flush.max-timeout";

	private final Map<String, String> userConfig;

	protected final int rowKeyIndex;
	protected final String[] fieldNames;
	protected final TypeInformation<?>[] fieldTypes;
	protected final String[] columnFamilies;
	protected final String[] qualifiers;
	protected final int[] fieldElementIndexMapping;

	/** The timer that triggers periodic flush to HBase. */
	private ScheduledThreadPoolExecutor executor;

	private ExecutorService flushExecutor;

	/** The lock to safeguard the flush commits. */
	private transient Object lock;

	private Connection connection;
	private transient Table hTable;
	private boolean isBuildingTable = false;

	private HBaseClientWrapper client;
	private List<Mutation> mutaionBuffer = new LinkedList<>();
	private long estimateSize = 0;

	private final boolean batchFlushEnable;
	private long batchFlushMaxMutations;
	private long batchFlushMaxSizeInBits;
	private long batchFlushIntervalMillis;
	private int batchFlushMaxRetries;
	private long batchFlushMaxTimeoutMillis;

	private boolean isRunning = false;

	public HBaseSinkFunctionBase(
		String clusterKey,
		String tableName,
		Map<String, String> userConfig,
		int rowKeyIndex,
		String[] outputFieldNames,
		String[] fieldNames,
		String[] columnFamilies,
		String[] qualifiers,
		TypeInformation<?>[] fieldTypes) {
		this(null, userConfig, rowKeyIndex, outputFieldNames, fieldNames, columnFamilies, qualifiers, fieldTypes);
		this.client = new HBaseClientWrapper().clusterKey(clusterKey).tableName(tableName);
		this.isBuildingTable = true;
	}

	public HBaseSinkFunctionBase(
		Table hTable,
		Map<String, String> userConfig,
		int rowKeyIndex,
		String[] outputFieldNames,
		String[] fieldNames,
		String[] columnFamilies,
		String[] qualifiers,
		TypeInformation<?>[] fieldTypes){
		this.connection = null;
		this.hTable = hTable;
		this.userConfig = userConfig;
		this.rowKeyIndex = rowKeyIndex;
		this.fieldNames = fieldNames;
		this.columnFamilies = columnFamilies;
		this.qualifiers = qualifiers;
		this.fieldTypes = fieldTypes;

		int[] fieldElementIndexMapping = new int[fieldNames.length];
		for (int i = 0; i < fieldNames.length; i++) {
			fieldElementIndexMapping[i] = -1;
			for (int j = 0; j < outputFieldNames.length; j++) {
				if (fieldNames[i].equals(outputFieldNames[j])) {
					fieldElementIndexMapping[i] = j;
					break;
				}
			}
			if (fieldElementIndexMapping[i] == -1) {
				throw new RuntimeException("The field " + outputFieldNames[i] + " is not found in the result stream.");
			}
		}
		this.fieldElementIndexMapping = fieldElementIndexMapping;

		batchFlushEnable = userConfig.getOrDefault(CONFIG_KEY_BATCH_FLUSH_ENABLE, "false").equals("true");
		batchFlushMaxMutations = Long.parseLong(userConfig.getOrDefault(CONFIG_KEY_BATCH_FLUSH_MAX_MUTATIONS, "128"));
		batchFlushMaxSizeInBits = Long.parseLong(userConfig.getOrDefault(CONFIG_KEY_BATCH_FLUSH_MAX_SIZE_MB, "2")) * 1024 * 1024 * 8;
		batchFlushIntervalMillis = Long.parseLong(userConfig.getOrDefault(CONFIG_KEY_BATCH_FLUSH_INTERVAL_MS, "1000"));
		batchFlushMaxRetries = Integer.parseInt(userConfig.getOrDefault(CONFIG_KEY_BATCH_FLUSH_MAX_RETRIES, "3"));
		batchFlushMaxTimeoutMillis = Long.parseLong(userConfig.getOrDefault(CONFIG_KEY_BATCH_FLUSH_MAX_TIMEOUT_MS, "5000"));
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		this.lock = new Object();
		if (isBuildingTable) {
			if (client != null) {
				this.connection = client.buildConnection();
			}
			if (connection != null) {
				this.hTable = client.buildTable(connection);
			}
		}
		if (hTable == null) {
			throw new RuntimeException("Cannot build connection for hbase sink, please check the configuraiton.");
		}
		if (batchFlushEnable) {
			((HTable) hTable).setAutoFlush(false, false);
		} else {
			((HTable) hTable).setAutoFlush(true, false);
		}
		this.executor = new ScheduledThreadPoolExecutor(1);
		this.flushExecutor = Executors.newFixedThreadPool(3);
		if (batchFlushEnable && batchFlushIntervalMillis > 0) {
			executor.scheduleAtFixedRate(() -> {
				if (this.hTable != null && this.hTable instanceof HTable) {
					synchronized (lock) {
						try {
							flushToHBaseWithRetryAndTimeout();
						} catch (Exception e){
							log.warn("Scheduled flush operation to HBase cannot be finished.", e);
						}
					}
				}
			}, batchFlushIntervalMillis, batchFlushIntervalMillis, TimeUnit.MILLISECONDS);
		}
		isRunning = true;
	}

	@Override
	public void invoke(IN value, Context context) throws Exception {
		Mutation mutation = extract(value);
		long mutationSize = mutation.heapSize();
		if (batchFlushEnable) {
			if (estimateSize != 0 && (estimateSize + mutationSize > batchFlushMaxSizeInBits || mutaionBuffer.size() + 1 > batchFlushMaxMutations)) {
				synchronized (lock){
					if (estimateSize != 0 && (estimateSize + mutationSize > batchFlushMaxSizeInBits
						|| mutaionBuffer.size() + 1 > batchFlushMaxMutations)) {
						long start = System.currentTimeMillis();
						Exception testException = null;
						try {
							flushToHBaseWithRetryAndTimeout();
						} catch (Exception e) {
							testException = e;
						}
						long end = System.currentTimeMillis();
						log.debug("Flush tasks " + (end - start) + " milliseconds with exception: " + testException);
					}
				}
			}
			synchronized (lock) {
				mutaionBuffer.add(mutation);
				estimateSize += mutation.heapSize();
			}
		} else if (mutation instanceof Put){
			hTable.put((Put) mutation);
		} else if (mutation instanceof Delete) {
			hTable.delete((Delete) mutation);
		} else if (mutation instanceof Append) {
			hTable.append((Append) mutation);
		} else if (mutation instanceof Increment) {
			hTable.increment((Increment) mutation);
		}
	}

	protected abstract Mutation extract(IN value);

	protected Put generatePutMutation(IN value) {
		byte[] rowKey = HBaseUtils.serialize(fieldTypes[rowKeyIndex], produceElementWithIndex(value, rowKeyIndex));
		Put put = new Put(rowKey);
		for (int i = 0; i < fieldNames.length; i++) {
			if (i != rowKeyIndex) {
				Object fieldValue = produceElementWithIndex(value, i);
				if (fieldValue != null) {
					put.addColumn(columnFamilies[i].getBytes(), qualifiers[i].getBytes(), HBaseUtils.serialize(fieldTypes[i], fieldValue));
				}
			}
		}
		return put;
	}

	protected Delete generateDeleteMutation(IN value) {
		byte[] rowKey = HBaseUtils.serialize(fieldTypes[rowKeyIndex], produceElementWithIndex(value, rowKeyIndex));
		Delete delete = new Delete(rowKey);
		for (int i = 0; i < fieldNames.length; i++) {
			if (i != rowKeyIndex) {
				delete.addColumn(columnFamilies[i].getBytes(), qualifiers[i].getBytes());
			}
		}
		return delete;
	}

	protected abstract Object produceElementWithIndex(IN value, int index);

	// The HBase client operation timeout doesn't include the time of getting server state from zookeeper.
	private void flushToHBaseWithRetryAndTimeout() throws ExecutionException, InterruptedException {
		long start = System.currentTimeMillis();
		try {
			CompletableFuture<Void> flushFuture = FutureUtils
				.retry(() -> CompletableFuture.runAsync(() -> flushToHBase(), flushExecutor), batchFlushMaxRetries, flushExecutor);
			if (!flushFuture.isDone()) {
				log.debug("start to wait for flush futre");
				flushFuture.get(batchFlushMaxTimeoutMillis, TimeUnit.MILLISECONDS);
			}
		} catch (TimeoutException e) {
			throw new RuntimeException("Flush operation to HBase cannot be finished.", e);
		}
		long end = System.currentTimeMillis();
		log.debug("takes " + (end - start) + " ms");
	}

	private void flushToHBase() {
		try {
			if (isRunning && mutaionBuffer.size() > 0) {
				if (hTable == null) {
					log.error("HBase table cannot be null during flush.");
				} else {
					log.debug("mutation size is " + mutaionBuffer.size());
					hTable.batch(mutaionBuffer, new Object[mutaionBuffer.size()]);
					mutaionBuffer.clear();
					estimateSize = 0;
				}
			}
		} catch (Exception e) {
			log.warn("Fail to flush data into HBase due to: ", e);
			throw new RuntimeException(e);
		}
	}

	@Override public void close() throws Exception {
		super.close();
		isRunning = false;
		try {
			if (this.hTable != null) {
				this.hTable.close();
				this.hTable = null;
			}
		} catch (Throwable t) {
			log.error("Error while closing HBase table.", t);
		}
		try {
			if (this.connection != null) {
				this.connection.close();
				this.connection = null;
			}
		} catch (Throwable t) {
			log.error("Error while closing HBase connection.", t);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws ExecutionException, InterruptedException {
		if (batchFlushEnable && this.hTable != null && this.hTable instanceof HTable) {
			synchronized (lock) {
				flushToHBaseWithRetryAndTimeout();
			}
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception { }

}
