package org.apache.flink.streaming.api.operators.lookup;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;

/**
 * Created by Shimin Yang on 2018/8/9.
 */
public class LookUpJoinOperator<K, IN1, IN2, OUT>
	extends AbstractUdfStreamOperator<OUT, JoinFunction<IN1, IN2, OUT>>
	implements OneInputStreamOperator<IN1, OUT>{

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	private final StateDescriptor<? extends MapState<K, LookUpCacheNode<IN2>>, ?> cacheStateDescriptor;

	protected final KeySelector<IN1, K> keySelector1;

	protected final KeySelector<IN2, K> keySelector2;

	protected final TypeSerializer<K> keySerializer;

	private final LookupService<K, IN2> lookupService;

	// ------------------------------------------------------------------------
	// State that is not checkpointed
	// ------------------------------------------------------------------------

	private transient InternalMapState<K, VoidNamespace, K, LookUpCacheNode<IN2>> cacheState;

	protected transient TimestampedCollector<OUT> timestampedCollector;

	// ------------------------------------------------------------------------
	// State that needs to be checkpointed
	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@code LookUpJoinOperator} based on the given policies and user functions.
	 */
	public LookUpJoinOperator(JoinFunction<IN1, IN2, OUT> userFunction,
		StateDescriptor<? extends MapState<K, LookUpCacheNode<IN2>>, ?> cacheStateDescriptor,
		KeySelector<IN1, K> keySelector1, KeySelector<IN2, K> keySelector2, TypeSerializer<K> keySerializer,
		LookupService<K, IN2> lookupService) {
		super(userFunction);
		this.cacheStateDescriptor = cacheStateDescriptor;
		this.keySelector1 = keySelector1;
		this.keySelector2 = keySelector2;
		this.keySerializer = keySerializer;
		this.lookupService = lookupService;

		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	@Override public void open() throws Exception {
		super.open();

		timestampedCollector = new TimestampedCollector<>(output);

		if (cacheStateDescriptor != null) {
			cacheState = (InternalMapState<K, VoidNamespace, K, LookUpCacheNode<IN2>>)
				getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, cacheStateDescriptor);
		}
		cacheState.setCurrentNamespace(VoidNamespace.INSTANCE);
	}

	@Override public void processElement(StreamRecord<IN1> element) throws Exception {
		IN1 in1 = element.getValue();
		K key = keySelector1.getKey(in1);
		LookUpCacheNode<IN2> cacheNode = cacheState.get(key);
		IN2 in2 = null;
		long currentProcessingTime = getProcessingTimeService().getCurrentProcessingTime();
		if (cacheNode != null && cacheNode.getTimestamp() >= currentProcessingTime) {
			in2 = cacheNode.getValue();
		} else {
			in2 = lookupService.getValueByKey(key);
			if (cacheNode == null) {
				cacheNode = new LookUpCacheNode<>(currentProcessingTime, in2);
			}
			cacheNode.setValue(in2);
			cacheNode.setTimestamp(currentProcessingTime + lookupService.getValidTimeout());
			cacheState.put(key, cacheNode);
		}
		OUT out = userFunction.join(in1, in2);
		// TODO set timestamp
		timestampedCollector.collect(out);
	}

	@Override public void close() throws Exception {
		super.close();
		timestampedCollector = null;

	}

	@Override public void dispose() throws Exception {
		super.dispose();
		timestampedCollector = null;

	}

	public static class LookUpCacheNode<IN2> implements Serializable{

		private Long timestamp;
		private IN2 value;

		public LookUpCacheNode(Long timestamp, IN2 value) {
			this.timestamp = timestamp;
			this.value = value;
		}

		public Long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(Long timestamp) {
			this.timestamp = timestamp;
		}

		public IN2 getValue() {
			return value;
		}

		public void setValue(IN2 value) {
			this.value = value;
		}
	}

	@Override public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
	}
}
