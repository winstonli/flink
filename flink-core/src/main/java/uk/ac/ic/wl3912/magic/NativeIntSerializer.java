package uk.ac.ic.wl3912.magic;

/**
 * Created by winston on 28/05/2016.
 */
public class NativeIntSerializer {

	static {
		System.loadLibrary("hmagic");
	}

	public static native int deserialize(byte[] source, long offset, long end, long[] numReadPtr);

}
