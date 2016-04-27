package org.apache.flink.runtime.io.network.netty;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Created by winston on 16/04/2016.
 */
public class Tuple2Record {

	public static long sizeof;

	private static long offsetof_key;
	private static long offsetof_utf16_len;
	private static long offsetof_utf16_big_end_chars;

	private static Field value;
	private static Field hash;
	private static Field modifiers;

	static {
		sizeof = sizeof();
		offsetof_key = offsetof_key();
		offsetof_utf16_len = offsetof_utf16_len();
		offsetof_utf16_big_end_chars = offsetof_utf16_big_end_chars();

		try {
			value = String.class.getDeclaredField("value");
			hash = String.class.getDeclaredField("hash");
			modifiers = Field.class.getDeclaredField("modifiers");
            modifiers.setAccessible(true);
            modifiers.setInt(value, value.getModifiers() & ~Modifier.FINAL);
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		value.setAccessible(true);
		hash.setAccessible(true);
	}

	private static native int sizeof();

	private static native long offsetof_key();

	private static native long offsetof_utf16_len();

	private static native long offsetof_utf16_big_end_chars();

	public static int key(long ptr) {
		return DiffingoObj.unsafe.getInt(ptr + offsetof_key);
	}

	public static int utf16_len(long ptr) {
		return DiffingoObj.unsafe.getInt(ptr + offsetof_utf16_len);
	}

	public static String copyOfString(long ptr) {
		return new String(DiffingoObj.copyCharsToArray(DiffingoObj.unsafe.getAddress(ptr + offsetof_utf16_big_end_chars), utf16_len(ptr)));
//		String str = new String();
//		try {
//			value.set(str, DiffingoObj.copyCharsToArray(DiffingoObj.unsafe.getAddress(ptr + offsetof_utf16_big_end_chars), utf16_len(ptr)));
//			hash.setInt(str, 0);
//		} catch (IllegalAccessException e) {
//			e.printStackTrace();
//			throw new RuntimeException(e);
//		}
//		return str;
	}

}
