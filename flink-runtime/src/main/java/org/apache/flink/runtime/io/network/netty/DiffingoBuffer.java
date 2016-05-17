package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MagicInputView;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * Created by winston on 16/05/2016.
 */
public class DiffingoBuffer extends Buffer implements MagicInputView {

	static final long sizeof_ptr = 8;

	private final long rec_arr;
	private final long arr_end;
	private final long kmagic_socket;
	private final long msg_resource;
	private long curr;

	public DiffingoBuffer(long rec_arr, int arr_len, long kmagic_socket, long msg_resource) {
		this.rec_arr = rec_arr;
		arr_end = rec_arr + sizeof_ptr * arr_len;
		this.kmagic_socket = kmagic_socket;
		this.msg_resource = msg_resource;
		curr = rec_arr;
	}

	public boolean hasRemaining() {
		return curr < arr_end;
	}

	public long next() {
		long ret = curr;
		curr += sizeof_ptr;
		return ret;
	}

	public long index(int i) {
		return rec_arr + i * sizeof_ptr;
	}

	public void delete() {
		DiffFlinkRecord.delete(kmagic_socket, msg_resource);
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
		((Tuple2) target).f0 = DiffFlinkRecord.copyOutOfField0(next);
		((Tuple2) target).f1 = DiffFlinkRecord.copyOutOfField1(next);
//		Tuple2Record.fill(next, ((Tuple2) target));
		return target;
	}

}
