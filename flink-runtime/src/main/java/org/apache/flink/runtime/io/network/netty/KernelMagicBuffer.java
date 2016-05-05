package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MagicInputView;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by winston on 16/04/2016.
 */
public class KernelMagicBuffer extends Buffer implements MagicInputView {

	public static final AtomicInteger remainingBufs = new AtomicInteger(0);
	public static final AtomicInteger totalBufs = new AtomicInteger(0);

	private final long buf;
	private long current;
	private final long last;
	private final int len;

	public KernelMagicBuffer(long buf, int len) {
		remainingBufs.addAndGet(len);
		totalBufs.addAndGet(len);
		this.buf = buf;
		current = buf;
		last = buf + len * Tuple2Record.sizeof;
		this.len = len;
	}

	public boolean hasRemaining() {
		return current < last;
	}

	public long next() {
		remainingBufs.decrementAndGet();
		long ret = current;
		current += Tuple2Record.sizeof;
		return ret;
	}

	public long index(int i) {
		return buf + i * Tuple2Record.sizeof;
	}

	public int len() {
		return len;
	}

	@Override
	public void recycle() {

	}

	@Override
	public void skipBytesToRead(int numBytes) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int read(byte[] b) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int skipBytes(int n) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean readBoolean() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte readByte() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int readUnsignedByte() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public short readShort() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public char readChar() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int readInt() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long readLong() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public float readFloat() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public double readDouble() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readLine() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readUTF() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object read() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object read(Object target) {
		long next = next();
		((Tuple2) target).f0 = Tuple2Record.key(next);
		((Tuple2) target).f1 = Tuple2Record.copyOfString(next);
		((Tuple2) target).magic = true;
		return target;
	}
}