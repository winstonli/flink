package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;

/**
 * Created by winston on 17/04/2016.
 */
public class MagicTupleSerializer<T extends Tuple> extends TupleSerializer<T> {

	public MagicTupleSerializer(Class tupleClass, TypeSerializer[] fieldSerializers) {
		super(tupleClass, fieldSerializers);
	}

	@Override
	public T createInstance() {
		return (T) new MagicTuple();
	}



}
