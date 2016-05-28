package uk.ac.ic.wl3912.magic;

/**
 * Created by winston on 28/05/2016.
 */
public class NativeStringSerializer {

	public static native String deserialize(byte[] source, long offset, long end, long[] numReadPtr);

}
