package org.apache.flink.runtime.operators;

/**
 * Created by winston on 24/05/2016.
 */
public class csv_record {

	static {
		offsetof_f0 = offsetof_f0();
		offsetof_f1 = offsetof_f1();
		offsetof_f2_str = offsetof_f2_str();
		offsetof_f2_len = offsetof_f2_len();
	}

	public static long offsetof_f0;
	public static long offsetof_f1;
	public static long offsetof_f2_str;
	public static long offsetof_f2_len;

	private static native long offsetof_f0();
	private static native long offsetof_f1();
	private static native long offsetof_f2_str();
	private static native long offsetof_f2_len();

}
