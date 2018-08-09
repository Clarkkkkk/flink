package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.lookup.LookUpJoinOperator;
import org.apache.flink.streaming.api.operators.lookup.LookupService;

import static java.util.Objects.requireNonNull;

/**
 * Created by Shimin Yang on 2018/8/9.
 */
public class LookupJoinedStream<T1, T2, KEY> {

	private final DataStream<T1> input1;

	private final LookupService<KEY, T2> lookupService;

	public LookupJoinedStream(DataStream<T1> input1, LookupService<KEY, T2> lookupService) {
		this.input1 = input1;
		this.lookupService = lookupService;
	}

	public Where<KEY> where(KeySelector<T1, KEY> keySelector) {
		TypeInformation<KEY> keyType = TypeExtractor.getKeySelectorTypes(keySelector, input1.getType());
		return new Where<>(input1.clean(keySelector), keyType, input1.keyBy(keySelector));
	}

	@Public
	public class Where<KEY> {

		private final KeySelector<T1, KEY> keySelector1;
		private final TypeInformation<KEY> keyType;
		private final KeyedStream<T1, KEY> keyedInput1;

		Where(KeySelector<T1, KEY> keySelector1, TypeInformation<KEY> keyType, KeyedStream<T1, KEY> keyedInput1) {
			this.keySelector1 = keySelector1;
			this.keyType = keyType;
			this.keyedInput1 = keyedInput1;
		}

		public EqualTo equalTo(KeySelector<T2, KEY> keySelector) {
			return new EqualTo(input1.clean(keySelector), keyedInput1);
		}

		@Public
		public class EqualTo {

			private final KeySelector<T2, KEY> keySelector2;
			private final KeyedStream<T1, KEY> keyedInput1;

			EqualTo(KeySelector<T2, KEY> keySelector2, KeyedStream<T1, KEY> keyedInput1) {
				this.keySelector2 = requireNonNull(keySelector2);
				this.keyedInput1 = keyedInput1;
			}

			public <T> DataStream<T> apply(JoinFunction<T1, T2, T> function) {
				function = keyedInput1.getExecutionEnvironment().clean(function);
				TypeInformation<T> outTypeInfo = TypeExtractor
					.getBinaryOperatorReturnType(function, JoinFunction.class, 0, 1, 2, new int[] { 0 }, new int[] { 1 },
						new int[] { 2 }, keyedInput1.getType(), lookupService.getType(), "LookupJoin", false);

				TypeInformation<LookUpJoinOperator.LookUpCacheNode<T2>> cacheNodeType = TypeInformation.of(
					new TypeHint<LookUpJoinOperator.LookUpCacheNode<T2>>() {
				});
				TypeSerializer<KEY> keyTypeSerializer = keyType.createSerializer(getExecutionEnvironment().getConfig());
				MapStateDescriptor<KEY, LookUpJoinOperator.LookUpCacheNode<T2>> mapDesc = new MapStateDescriptor<>(
					"Cache-contents",
					keyTypeSerializer,
					cacheNodeType.createSerializer(getExecutionEnvironment().getConfig())
				);

				LookUpJoinOperator<KEY, T1, T2, T> lookUpJoinOperator =
					new LookUpJoinOperator(function, mapDesc, keySelector1, keySelector2, keyTypeSerializer, lookupService);
				// TODO Operator name
				return keyedInput1.transform("LookupJoin", outTypeInfo, lookUpJoinOperator);
			}
		}
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return input1.getExecutionEnvironment();
	}
}
