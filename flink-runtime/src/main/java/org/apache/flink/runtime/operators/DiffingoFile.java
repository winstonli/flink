package org.apache.flink.runtime.operators;

import org.apache.flink.runtime.io.network.netty.DiffingoObj;

/**
 * Created by winston on 23/05/2016.
 */
public class DiffingoFile {

	static {
		offsetof_arr_res = offsetof_arr_res();
		offsetof_arr_res_size = offsetof_arr_res_size();
	}

	public static long offsetof_arr_res;

	public static long offsetof_arr_res_size;

	public static native long construct(String path, int buf_size, int batch_size, int stack_size);

	public static native void delete(long diffingo_file);

	public static native void open_split(long diffingo_file, long offset, long len);

	public static native void do_read(long diffingo_file);

	private static native long offsetof_arr_res();

	private static native long offsetof_arr_res_size();

	public static long arr_res(long diffingo_file) {
		return DiffingoObj.unsafe.getAddress(diffingo_file + offsetof_arr_res);
	}

	public static int arr_res_size(long diffingo_file) {
		return DiffingoObj.unsafe.getInt(diffingo_file + offsetof_arr_res_size);
	}

}
