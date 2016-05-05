package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MagicInputView;

import java.io.IOException;

/**
 * Created by winston on 02/05/2016.
 */
public class ArrayMagicBuffer<T> extends Buffer implements MagicInputView<T> {

	private T[] arr;
	private int currentIndex;

	public ArrayMagicBuffer(T[] arr) {
		this.arr = arr;
		currentIndex = 0;
	}

	@Override
	public void recycle() {
		arr = null;
	}

	public boolean hasRemaining() {
		return currentIndex < arr.length;
	}

	@Override
	public T read() {
		return arr[currentIndex++];
	}

	@Override
	public T read(T target) {
		return arr[currentIndex++];
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
