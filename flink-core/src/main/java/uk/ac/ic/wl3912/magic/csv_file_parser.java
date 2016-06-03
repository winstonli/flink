package uk.ac.ic.wl3912.magic;

/**
 * Created by winston on 02/06/2016.
 */
public class csv_file_parser {

	static {
		System.loadLibrary("webjob_jni");
	}

	public static native long create(String path);

	public static native void delete(long p_this);

	public static native void open_split(long p_this, long offset, long len);

	public static native boolean read(
			long p_this,
			char[] url,
			int[] url_len_ptr,
			char[] link,
			int[] link_len_ptr
	);

}
