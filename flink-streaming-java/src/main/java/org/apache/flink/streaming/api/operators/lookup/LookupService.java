package org.apache.flink.streaming.api.operators.lookup;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Created by Shimin Yang on 2018/8/9.
 */
public interface LookupService<KEY, VALUE> {

	VALUE getValueByKey(KEY key);

	long getValidTimeout();

	TypeInformation<VALUE> getType();
}
