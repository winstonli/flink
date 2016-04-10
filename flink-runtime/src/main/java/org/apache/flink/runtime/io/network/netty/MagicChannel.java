package org.apache.flink.runtime.io.network.netty;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * Created by winston on 10/04/2016.
 */
public class MagicChannel implements Channel {

	private final MagicParser parser;
	private final PartitionRequestClientHandler handler;

	private Socket socket;
	private InputStream inputStream;
	private OutputStream outputStream;

	public MagicChannel(MagicParser parser, PartitionRequestClientHandler handler) {
		this.parser = parser;
		this.handler = handler;
	}

	@Override
	public EventLoop eventLoop() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Channel parent() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelConfig config() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isOpen() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isRegistered() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isActive() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelMetadata metadata() {
		throw new UnsupportedOperationException();
	}

	@Override
	public SocketAddress localAddress() {
		throw new UnsupportedOperationException();
	}

	@Override
	public SocketAddress remoteAddress() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture closeFuture() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isWritable() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Unsafe unsafe() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline pipeline() {
		return new MagicPipeline(handler);
	}

	@Override
	public ByteBufAllocator alloc() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPromise newPromise() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelProgressivePromise newProgressivePromise() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture newSucceededFuture() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture newFailedFuture(Throwable cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPromise voidPromise() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture bind(SocketAddress localAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture disconnect() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture close() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture deregister() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture disconnect(ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture close(ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture deregister(ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Channel read() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture write(Object msg) {
		MagicPromise ret = new MagicPromise(this);
		if (msg instanceof NettyMessage.PartitionRequest) {
			NettyMessage.PartitionRequest pr = ((NettyMessage.PartitionRequest) msg);
			int len = NettyMessage.HEADER_LENGTH + 16 + 16 + 4 + 16;
			ByteBuffer buf = ByteBuffer.allocate(16);
			ByteBuf bbuf = Unpooled.wrappedBuffer(buf);
			bbuf.resetWriterIndex();
			try {
				buf.putInt(0, len);
				outputStream.write(buf.array(), 0, 4);

				buf.putInt(0, NettyMessage.MAGIC_NUMBER);
				outputStream.write(buf.array(), 0, 4);

				outputStream.write(NettyMessage.PartitionRequest.ID);

				pr.partitionId.getPartitionId().writeTo(bbuf);
				bbuf.resetWriterIndex();
				outputStream.write(buf.array(), 0, 16);

				pr.partitionId.getProducerId().writeTo(bbuf);
				bbuf.resetWriterIndex();
				outputStream.write(buf.array(), 0, 16);

				buf.putInt(0, pr.queueIndex);
				outputStream.write(buf.array(), 0, 4);

				pr.receiverId.writeTo(bbuf);
				bbuf.resetWriterIndex();
				outputStream.write(buf.array(), 0, 16);
				return ret.setSuccess();
			} catch (IOException e) {
				return ret.setFailure(e);
			} finally {
				bbuf.release();
			}
		} else if (msg instanceof NettyMessage.CloseRequest) {
			int len = NettyMessage.HEADER_LENGTH;
			ByteBuffer buf = ByteBuffer.allocate(4);
			try {
				buf.putInt(0, len);
				outputStream.write(buf.array(), 0, 4);
				buf.putInt(0, NettyMessage.MAGIC_NUMBER);
				outputStream.write(buf.array(), 0, 4);
				outputStream.write(5); /* CloseRequest.ID */
				return ret.setSuccess();
			} catch (IOException e) {
				return ret.setFailure(e);
			}
		}
		throw new UnsupportedOperationException("Can't write class: " + msg.getClass().getSimpleName());
	}

	@Override
	public ChannelFuture write(Object msg, ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Channel flush() {
		try {
			outputStream.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return this;
	}

	@Override
	public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture writeAndFlush(Object msg) {
		ChannelFuture ret = write(msg);
		flush();
		return ret;
	}

	@Override
	public <T> Attribute<T> attr(AttributeKey<T> key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int compareTo(Channel o) {
		throw new UnsupportedOperationException();
	}

	public void setSocket(Socket socket) throws IOException {
		this.socket = socket;
		inputStream = socket.getInputStream();
		outputStream = socket.getOutputStream();
		new Thread() {

			PooledByteBufAllocator pool = new PooledByteBufAllocator();

			ByteBuffer header = ByteBuffer.allocate(NettyMessage.HEADER_LENGTH);

			@Override
			public void run() {
				try {
					doRead();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			private void doRead() throws IOException {
				while (inputStream.read(header.array(), 0, NettyMessage.HEADER_LENGTH) == NettyMessage.HEADER_LENGTH) {
					int frameLen = header.getInt(0);
					int magic = header.getInt(4);
					Preconditions.checkArgument(magic == NettyMessage.MAGIC_NUMBER);
					int id = header.get(8);
					switch (id) {
					case 0: /* BufferResponse */
						doReadBufferResponse(frameLen);
					case 1: /* ErrorResponse */
						break;
//						throw new UnsupportedOperationException();
					default:
						throw new IllegalStateException("id was wrong");
					}
				}
			}

			private void doReadBufferResponse(int frameLen) throws IOException {
				int bodyLen = frameLen - NettyMessage.HEADER_LENGTH;
				ByteBuf bbuf = pool.heapBuffer(bodyLen);
				try {
					int currentRead;
					int read = 0;
					while ((currentRead = inputStream.read(bbuf.array(), bbuf.arrayOffset() + bbuf.writerIndex(), bodyLen - read)) != -1) {
						read += currentRead;
						bbuf.writerIndex(bbuf.writerIndex() + currentRead);
						if (read == bodyLen) {
							break;
						} else if (read >= bodyLen) {
							throw new IllegalStateException("read too much");
						}
					}
                    InputChannelID receiverId = InputChannelID.fromByteBuf(bbuf);
                    int sequenceNumber = bbuf.readInt();
                    boolean isBuffer = bbuf.readBoolean();
					int size = bbuf.readInt();
					if (isBuffer) {
						IOReadableWritable p = handler.getParserForChannelId(receiverId);
						Buffer b = parser.parse(bbuf, size, p, receiverId);
						handler.getInputChannelForId(receiverId).onBuffer(b, sequenceNumber);
					} else {
						byte[] copy = new byte[size];
						bbuf.readBytes(copy);
						MemorySegment memSeg = MemorySegmentFactory.wrap(copy);
						Buffer buffer = new Buffer(memSeg, FreeingBufferRecycler.INSTANCE, false);
						handler.getInputChannelForId(receiverId).onBuffer(buffer, sequenceNumber);
					}
                } finally {
                    bbuf.release();
                }
			}

		}.start();
	}

}
