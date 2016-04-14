package org.apache.flink.runtime.io.network.netty;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.io.network.buffer.MagicBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.plugable.DeserializationDelegate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * Created by winston on 05/04/2016.
 */
public class FlinkMagicParser {

	Map<InputChannelID, SpillingBuffer> unfinished = new HashMap<>();


	public synchronized MagicBuffer parse(ByteBuf nettyBuffer, int len, IOReadableWritable ioReadableWritable, InputChannelID receiverId) throws IOException {
		SpillingBuffer buf = unfinished.get(receiverId);
		if (buf == null) {
			buf = new SpillingBuffer();
		}
		buf.add(new BBuf(nettyBuffer, len));
		Queue<Object> magics = new ArrayDeque<>();
		while (buf.hasNext()) {
			try {
				ioReadableWritable.read(buf);
			} catch (Throwable t) {
				t.printStackTrace();
			}
			magics.add(((Tuple2) ((DeserializationDelegate<?>) ioReadableWritable).getInstance()).copy());
		}
		if (buf.isSplit()) {
			unfinished.put(receiverId, buf);
		} else {
			unfinished.remove(receiverId);
		}
		return new MagicBuffer<>(magics, buf.isSplit());
	}

	static class BBuf {

		private final ByteBuf buf;
		private final int len;

		BBuf(ByteBuf buf, int len) {
			this.buf = buf;
			this.len = buf.readerIndex() + len;
		}

		public ByteBuf getBuf() {
			return buf;
		}

		public int getLen() {
			return len;
		}

		public int remaining() {
			return len - buf.readerIndex();
		}

	}

	static class SpillingBuffer implements DataInputView {

		private final Queue<BBuf> bufs = new ArrayDeque<>();
		private int currentRemaining = 0;

		synchronized void readLen() {
			if (currentRemaining > 0) {
				return;
			}
			int totalBytes = 0;
			for (BBuf buf : bufs) {
				totalBytes += buf.remaining();
				if (totalBytes >= 4) {
					break;
				}
			}
			if (totalBytes < 4) {
				return;
			}
			BBuf head = bufs.peek();
			if (head.remaining() >= 4) {
				currentRemaining = head.getBuf().readInt();
				if (head.remaining() == 0) {
					bufs.remove().getBuf().release();
				}
				return;
			}
			ByteBuffer countBuf = ByteBuffer.allocate(4);
			for (int i = 0; i < 4; ++i) {
				if (head.remaining() == 0) {
					bufs.remove().getBuf().release();
					head = bufs.peek();
				}
				Preconditions.checkState(head != null && head.remaining() > 0);
				countBuf.put(head.getBuf().readByte());
			}
			currentRemaining = countBuf.getInt(0);
		}

		public synchronized boolean hasNext() {
			readLen();
			if (currentRemaining == 0) {
				return false;
			}
			int totalRemaining = 0;
			for (BBuf buf : bufs) {
				totalRemaining += buf.remaining();
				if (totalRemaining >= currentRemaining) {
					return true;
				}
			}
			return false;
		}

		public synchronized boolean isSplit() {
			if (hasNext()) {
				return false;
			}
			for (BBuf buf : bufs) {
				if (buf.remaining() > 0) {
					return true;
				}
			}
			return currentRemaining > 0;
		}

		public synchronized void add(BBuf bbuf) {
			bufs.add(bbuf);
			bbuf.getBuf().retain();
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
			BBuf head = bufs.peek();
			int b = head.getBuf().readUnsignedByte();
			--currentRemaining;
			//
			if (head.remaining() == 0) {
				bufs.remove(head);
				head.getBuf().release();
			}
			if (currentRemaining < 0) {
				System.out.println();
			}
			return b;
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
			BBuf head = bufs.peek();
			if (head.remaining() >= 4) {
				currentRemaining -= 4;
				int i = head.getBuf().readInt();

				if (head.remaining() == 0) {
					bufs.remove().getBuf().release();
				}
				return i;
			}
			ByteBuffer buf = ByteBuffer.allocate(4);
			for (int i = 0; i < 4; ++i) {
				if (head.remaining() == 0) {
					bufs.remove().getBuf().release();
					head = bufs.peek();
				}
				Preconditions.checkState(head != null && head.remaining() > 0);
				buf.put(head.getBuf().readByte());
			}
			currentRemaining -= 4;
			return buf.get(0);
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

}
