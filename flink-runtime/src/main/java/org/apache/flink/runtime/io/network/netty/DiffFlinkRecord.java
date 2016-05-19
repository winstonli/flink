package org.apache.flink.runtime.io.network.netty;

import com.google.common.base.Preconditions;

/**
 * Created by winston on 16/05/2016.
 */
public class DiffFlinkRecord {

	static long offsetof_data;

	static {
		offsetof_record_data = offsetof_record_data();
		offsetof_field0 = offsetof_field0();
		offsetof_field1 = offsetof_field1();
		offsetof_data = offsetof_data();
		offsetof_utf16_ptr = offsetof_data + offsetof_data_();
		offsetof_utf16_len = offsetof_data + offsetof_len_();
	}

	public static native void init();

	public static long offsetof_record_data;

	public static long offsetof_field0;

	public static long offsetof_field1;

	public static long offsetof_utf16_ptr;

	public static long offsetof_utf16_len;

	public static native long offsetof_record_data();

	public static native long offsetof_field0();

	public static native long offsetof_field1();

	public static native long offsetof_data();

	public static native long offsetof_data_();

	public static native long offsetof_len_();

	public static native void delete(long socket, long flink_msg);

	public static int copyOutOfField0(long rec_ptr_ptr) {
		Preconditions.checkArgument(rec_ptr_ptr != 0);
		long rec_ptr = DiffingoObj.unsafe.getAddress(rec_ptr_ptr);
		Preconditions.checkArgument(rec_ptr != 0);
		long rec_data_ptr = DiffingoObj.unsafe.getAddress(rec_ptr + offsetof_record_data);
		Preconditions.checkArgument(rec_data_ptr != 0);
		return DiffingoObj.unsafe.getInt(rec_data_ptr + offsetof_field0);
	}

	public static String copyOutOfField1(long rec_ptr_ptr) {
		long rec_ptr = DiffingoObj.unsafe.getAddress(rec_ptr_ptr);
		long rec_data_ptr = DiffingoObj.unsafe.getAddress(rec_ptr + offsetof_record_data);
		long field1 = DiffingoObj.unsafe.getAddress(rec_data_ptr + offsetof_field1);
		long utf16_ptr = DiffingoObj.unsafe.getAddress(field1 + offsetof_utf16_ptr);
		int utf16_len = DiffingoObj.unsafe.getInt(field1 + offsetof_utf16_len);
		return new String(DiffingoObj.copyCharsToArray(utf16_ptr, utf16_len));
	}

}
