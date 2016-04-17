package org.apache.flink.runtime.io.network.netty;

/**
 * Created by winston on 16/04/2016.
 */
public class DiffingoOffset {

	public static native long offsetof_type();
	public static native long offsetof_data();

	public static native long offsetof_error_buf();
	public static native long offsetof_error_len();

	public static native long offsetof_event_uuidbytes();
	public static native long offsetof_event_seqnum();
	public static native long offsetof_event_size();
	public static native long offsetof_event_buf();
	public static native long offsetof_event_len();

	public static native long offsetof_buffer_uuidbytes();
	public static native long offsetof_buffer_seqnum();
	public static native long offsetof_buffer_size();
	public static native long offsetof_buffer_recordarr();
	public static native long offsetof_buffer_numrecords();

}
