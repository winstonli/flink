package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MagicInputView;

import java.io.IOException;
import java.util.Queue;

/**
 * Created by winston on 05/04/2016.
 */
public class MagicBuffer<T>
	extends Buffer
    implements MagicInputView<T> {

	private Queue<T> magics;
	private boolean split;

	public MagicBuffer(Queue<T> magics, boolean split) {
		this.magics = magics;
		this.split = split;
	}

	@Override
	public void recycle() {
		magics = null;
	}

	public boolean hasRemaining() {
		return !magics.isEmpty();
	}

	public boolean isSplit() {
		return split;
	}

	@Override
	public T read() {
		return magics.remove();
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

}
