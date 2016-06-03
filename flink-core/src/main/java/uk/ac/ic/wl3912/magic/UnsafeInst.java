package uk.ac.ic.wl3912.magic;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by winston on 02/06/2016.
 */
public class UnsafeInst {

	static {
		try {
			Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
			unsafeField.setAccessible(true);
			unsafe = (Unsafe) unsafeField.get(null);
		} catch (IllegalAccessException | NoSuchFieldException e) {
			e.printStackTrace();
		}
	}

	public static Unsafe unsafe;

}
