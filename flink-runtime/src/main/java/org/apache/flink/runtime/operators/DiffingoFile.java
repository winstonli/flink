package org.apache.flink.runtime.operators;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.io.network.netty.DiffingoObj;

/**
 * Created by winston on 23/05/2016.
 */
public class DiffingoFile {

	static {
		offsetof_arr_res = offsetof_arr_res();
		offsetof_arr_res_size = offsetof_arr_res_size();
		offsetof_rec = offsetof_rec();
	}

	public static long offsetof_arr_res;

	public static long offsetof_arr_res_size;

	public static long offsetof_rec;

	public static native long construct(String path, int buf_size, int batch_size, int stack_size);

	public static native void delete(long diffingo_file);

	public static native void open_split(long diffingo_file, long offset, long len);

	public static native void do_read(long diffingo_file);

	private static native long offsetof_arr_res();

	private static native long offsetof_arr_res_size();

	private static native long offsetof_rec();

	public static long arr_res(long diffingo_file) {
		return DiffingoObj.unsafe.getAddress(diffingo_file + offsetof_arr_res);
	}

	public static int arr_res_size(long diffingo_file) {
		return DiffingoObj.unsafe.getInt(diffingo_file + offsetof_arr_res_size);
	}

	public static native boolean do_handrolled_read(long diffingo_file);

	public static void readInto(long diffingo_file, Tuple3<Integer, Long, String> reuse) {
		long rec_ptr = diffingo_file + offsetof_rec;
		reuse.f0 = DiffingoObj.unsafe.getInt(rec_ptr + csv_record.offsetof_f0);
		reuse.f1 = DiffingoObj.unsafe.getLong(rec_ptr + csv_record.offsetof_f1);
		reuse.f2 = new String(
			DiffingoObj.copyCharsToArray(
				rec_ptr + csv_record.offsetof_f2_str,
				DiffingoObj.unsafe.getInt(rec_ptr + csv_record.offsetof_f2_len)
			)
		);
	}

	public static native boolean do_critical_read(long diffingo_file, int[] f0, long[] f1, char[] str);

}
