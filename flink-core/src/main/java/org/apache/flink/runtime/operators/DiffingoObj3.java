package org.apache.flink.runtime.operators;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.io.network.netty.DiffingoObj;

/**
 * Created by winston on 23/05/2016.
 */
public class DiffingoObj3 {

	static {
		offsetof_field0 = offsetof_field0();
		offsetof_field1 = offsetof_field1();
		offsetof_field2_data = offsetof_field2_data();
		offsetof_field2_len = offsetof_field2_len();
	}

	public static long offsetof_field0;

	public static long offsetof_field1;

	public static long offsetof_field2_data;

	public static long offsetof_field2_len;

	private static native long offsetof_field0();

	private static native long offsetof_field1();

	private static native long offsetof_field2_data();

	private static native long offsetof_field2_len();

	public static void readInto(long pThis, Tuple3<Integer, Long, String> reuse) {
		reuse.f0 = DiffingoObj.unsafe.getInt(pThis + offsetof_field0);
		reuse.f1 = DiffingoObj.unsafe.getLong(pThis + offsetof_field1);
		reuse.f2 = new String(
			DiffingoObj.copyCharsToArray(
				DiffingoObj.unsafe.getAddress(pThis + offsetof_field2_data),
				DiffingoObj.unsafe.getInt(pThis + offsetof_field2_len)
			)
		);
	}

}
