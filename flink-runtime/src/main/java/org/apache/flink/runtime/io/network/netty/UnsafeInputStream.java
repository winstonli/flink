package org.apache.flink.runtime.io.network.netty;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by winston on 16/04/2016.
 */
public class UnsafeInputStream extends InputStream {

	private int offset;
	private long ptr;
	private final int len;

	public UnsafeInputStream(long ptr, int len) {
		this.ptr = ptr;
		offset = 0;
		this.len = len;
	}

	public long getCurrentPos() {
		return ptr + offset;
	}

	public long getOffsetChecked(int offset) throws IOException {
		if (this.offset + offset >= len) {
			throw new IOException("out of bounds");
		}
		return ptr + this.offset + offset;
	}

	@Override
	public int read() throws IOException {
		if (offset >= len) {
			return -1;
		}
		return DiffingoObj.unsafe.getByte(ptr + offset++);
	}

}
