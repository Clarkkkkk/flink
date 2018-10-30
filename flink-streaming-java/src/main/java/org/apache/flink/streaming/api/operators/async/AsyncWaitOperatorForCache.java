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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;

/**
 * Created by Shimin Yang on 2018/10/30.
 */
public class AsyncWaitOperatorForCache<IN, OUT> extends AsyncWaitOperator<IN, OUT> {

	private static final String KEY_FOR_ALL_STATE = "key_for_all_state_in_async_operator_with_cache";

	public AsyncWaitOperatorForCache(
		AsyncFunction<IN, OUT> asyncFunction,
		long timeout,
		int capacity,
		AsyncDataStream.OutputMode outputMode) {
		super(asyncFunction, timeout, capacity, outputMode);
	}

	@Override public void setCurrentKey(Object key) {
		super.setCurrentKey(KEY_FOR_ALL_STATE);
	}
}
