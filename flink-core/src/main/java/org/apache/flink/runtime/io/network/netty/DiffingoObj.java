package org.apache.flink.runtime.io.network.netty;

import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by winston on 16/04/2016.
 */
public class DiffingoObj {

	public static Unsafe unsafe = null;
	private static ByteBuffer buf = ByteBuffer.allocateDirect(0).order(ByteOrder.BIG_ENDIAN);
	private static Field address;
	private static Field capacity;
	private static Field limit;
	private static long oldAddress;

	private static long byteArrayOffset;
	private static long charArrayOffset;

	private static long offsetof_type;
	private static long offsetof_data;

	private static long offsetof_error_buf;
	private static long offsetof_error_len;

	private static long offsetof_event_uuidbytes;
	private static long offsetof_event_seqnum;
	private static long offsetof_event_size;
	private static long offsetof_event_buf;
	private static long offsetof_event_len;

	private static long offsetof_buffer_uuidbytes;
	private static long offsetof_buffer_seqnum;
	private static long offsetof_buffer_size;
	private static long offsetof_buffer_recordarr;
	private static long offsetof_buffer_numrecords;

	static {
		System.loadLibrary("kmagic_jni");
		Constructor<Unsafe> unsafeConstructor;
		try {
			unsafeConstructor = Unsafe.class.getDeclaredConstructor();
			unsafeConstructor.setAccessible(true);
			unsafe = unsafeConstructor.newInstance();
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
			| IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
			System.exit(1);
		}

		try {
			address = Buffer.class.getDeclaredField("address");
            capacity = Buffer.class.getDeclaredField("capacity");
            limit = Buffer.class.getDeclaredField("limit");
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
			System.exit(1);
		}

		address.setAccessible(true);
		capacity.setAccessible(true);
		limit.setAccessible(true);

		try {
			oldAddress = address.getLong(buf);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		byteArrayOffset = unsafe.arrayBaseOffset(byte[].class);
		charArrayOffset = unsafe.arrayBaseOffset(char[].class);

		offsetof_type = DiffingoOffset.offsetof_type();
		offsetof_data = DiffingoOffset.offsetof_data();

		offsetof_error_buf = offsetof_data + DiffingoOffset.offsetof_error_buf();
		offsetof_error_len = offsetof_data + DiffingoOffset.offsetof_error_len();

		offsetof_event_uuidbytes = offsetof_data + DiffingoOffset.offsetof_event_uuidbytes();
		offsetof_event_seqnum = offsetof_data + DiffingoOffset.offsetof_event_seqnum();
		offsetof_event_size = offsetof_data + DiffingoOffset.offsetof_event_size();
		offsetof_event_buf = offsetof_data + DiffingoOffset.offsetof_event_buf();
		offsetof_event_len = offsetof_data + DiffingoOffset.offsetof_event_len();

		offsetof_buffer_uuidbytes = offsetof_data + DiffingoOffset.offsetof_buffer_uuidbytes();
		offsetof_buffer_seqnum = offsetof_data + DiffingoOffset.offsetof_buffer_seqnum();
		offsetof_buffer_size = offsetof_data + DiffingoOffset.offsetof_buffer_size();
		offsetof_buffer_recordarr = offsetof_data + DiffingoOffset.offsetof_buffer_recordarr();
		offsetof_buffer_numrecords = offsetof_data + DiffingoOffset.offsetof_buffer_numrecords();
	}

	@Override
	protected void finalize() throws Throwable {
		address.setLong(buf, oldAddress);
		capacity.setInt(buf, 0);
		limit.setInt(buf, 0);
		super.finalize();
	}

	public static long get8NoEndian(long ptr) {
		try {
			address.setLong(buf, ptr);
            capacity.setInt(buf, 8);
            limit.setInt(buf, 8);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return buf.getLong(0);
	}

	public static DiffingoObjType getType(long ptr) {
		return DiffingoObjType.values()[unsafe.getInt(ptr)];
	}

	public static long getErrorBuf(long ptr) {
		return unsafe.getAddress(ptr + offsetof_error_buf);
	}

	public static int getErrorLen(long ptr) {
		return unsafe.getInt(ptr + offsetof_error_len);
	}

	public static long getEventUuidLowerBytes(long ptr) {
		return get8NoEndian(ptr + offsetof_event_uuidbytes);
	}

	public static long getEventUuidUpperBytes(long ptr) {
		return get8NoEndian(ptr + offsetof_event_uuidbytes + 8);
	}

	public static int getEventSeqNum(long ptr) {
		return unsafe.getInt(ptr + offsetof_event_seqnum);
	}

	public static int getEventSize(long ptr) {
		return unsafe.getInt(ptr + offsetof_event_size);
	}

	public static long getEventBuf(long ptr) {
		return unsafe.getAddress(ptr + offsetof_event_buf);
	}

	public static byte[] copyBytesToArray(long ptr, int len) {
		byte[] bytes = new byte[len];
		unsafe.copyMemory(null, ptr, bytes, byteArrayOffset, len);
		return bytes;
	}

	public static char[] copyCharsToArray(long ptr, int len) {
		char[] chars = new char[len];
		unsafe.copyMemory(null, ptr, chars, charArrayOffset, len * 2);
		return chars;
	}

	public static int getEventLen(long ptr) {
		return unsafe.getInt(ptr + offsetof_event_len);
	}

	public static long getBufferUuidLowerBytes(long ptr) {
		return get8NoEndian(ptr + offsetof_buffer_uuidbytes);
	}

	public static long getBufferUuidUpperBytes(long ptr) {
		return get8NoEndian(ptr + offsetof_buffer_uuidbytes + 8);
	}

	public static int getBufferSeqNum(long ptr) {
		return unsafe.getInt(ptr + offsetof_buffer_seqnum);
	}

	public static int getBufferSize(long ptr) {
		return unsafe.getInt(ptr + offsetof_buffer_size);
	}

	public static long getBufferRecordArr(long ptr) {
		return unsafe.getAddress(ptr + offsetof_buffer_recordarr);
	}

	public static int getBufferNumRecords(long ptr) {
		return unsafe.getInt(ptr + offsetof_buffer_numrecords);
	}

	public static native void delete(long ptr);

	public enum DiffingoObjType {
		UNKNOWN,
		HEADER,
		ERROR,
		EVENT,
		BUFFER;
	}

}
