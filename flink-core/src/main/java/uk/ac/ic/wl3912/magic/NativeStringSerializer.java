package uk.ac.ic.wl3912.magic;

/**
 * Created by winston on 28/05/2016.
 */
public class NativeStringSerializer {

	static {
		System.loadLibrary("hmagic");
	}

	public static native long create();
	public static native void delete(long pThis);
	public static native String deserialize(
            long pThis,
            byte[] source,
            long offset,
            long end,
            long[] numReadPtr
	);
	public static native boolean deserializeChars(
            byte[] source,
            long offset,
            long end,
            char[] target,
            long[] lenPtr,
            long[] numReadPtr,
			long[] remainingPtr
	);

}
