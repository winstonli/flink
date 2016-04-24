package org.apache.flink.runtime.io.network.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * Created by winston on 16/04/2016.
 */
public class KernelMagicChannel implements Channel {

	private final PartitionRequestClientHandler handler;
	private MagicSocket socket;

	public KernelMagicChannel(PartitionRequestClientHandler handler) {
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
		FlinkMagicPromise ret = new FlinkMagicPromise(this);
		if (msg instanceof NettyMessage.PartitionRequest) {
			NettyMessage.PartitionRequest pr = ((NettyMessage.PartitionRequest) msg);
			int len = NettyMessage.HEADER_LENGTH + 16 + 16 + 4 + 16;
			ByteBuffer buf = ByteBuffer.allocate(16);
			ByteBuf bbuf = Unpooled.wrappedBuffer(buf);
			bbuf.resetWriterIndex();
			try {
				buf.putInt(0, len);
				socket.write(buf.array(), 0, 4);

				buf.putInt(0, NettyMessage.MAGIC_NUMBER);
				socket.write(buf.array(), 0, 4);

				socket.write(NettyMessage.PartitionRequest.ID);

				pr.partitionId.getPartitionId().writeTo(bbuf);
				bbuf.resetWriterIndex();
				socket.write(buf.array(), 0, 16);

				pr.partitionId.getProducerId().writeTo(bbuf);
				bbuf.resetWriterIndex();
				socket.write(buf.array(), 0, 16);

				buf.putInt(0, pr.queueIndex);
				socket.write(buf.array(), 0, 4);

				pr.receiverId.writeTo(bbuf);
				bbuf.resetWriterIndex();
				socket.write(buf.array(), 0, 16);
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
				socket.write(buf.array(), 0, 4);
				buf.putInt(0, NettyMessage.MAGIC_NUMBER);
				socket.write(buf.array(), 0, 4);
				socket.write(5); /* CloseRequest.ID */
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

	public void setSocket(final MagicSocket socket) throws IOException {
		this.socket = socket;
		new Thread() {

			@Override
			public void run() {
				try {
					while (true) {
						long res = socket.read();
						if (res == 0) {
							System.out.println("WINSTON-MAGIC socket read was nullptr");
							break;
						}
						DiffingoObj.DiffingoObjType type = DiffingoObj.getType(res);
						switch (type) {
						case UNKNOWN:
							throw new IllegalStateException("empty diffingo obj received");
						case HEADER:
							break;
						case ERROR:
							long buf = DiffingoObj.getErrorBuf(res);
							int len = DiffingoObj.getErrorLen(res);
							handleError(buf, len);
						case EVENT:
							long evLower = DiffingoObj.getEventUuidLowerBytes(res);
							long evUpper = DiffingoObj.getEventUuidUpperBytes(res);
							InputChannelID evUuid = new InputChannelID(evLower, evUpper);
							int evSeqNum = DiffingoObj.getEventSeqNum(res);
							int evSize = DiffingoObj.getEventSize(res);
							long evBuf = DiffingoObj.getEventBuf(res);
							int evLen = DiffingoObj.getEventLen(res);
							handleEvent(evUuid, evSeqNum, evSize, evBuf, evLen);
							break;
						case BUFFER:
							int bufSeqNum = DiffingoObj.getBufferSeqNum(res);
							long bufLower = DiffingoObj.getBufferUuidLowerBytes(res);
							long bufUpper = DiffingoObj.getBufferUuidUpperBytes(res);
							InputChannelID bufUuid = new InputChannelID(bufLower, bufUpper);
							int bufSize = DiffingoObj.getBufferSize(res);
							long bufRecords = DiffingoObj.getBufferRecordArr(res);
							int bufNumRecords = DiffingoObj.getBufferNumRecords(res);
							handleBuffer(bufUuid, bufSeqNum, bufSize, bufRecords, bufNumRecords);
							break;
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}.start();
	}

	private void handleTData(FlinkMagicObject tDataObj) {
		FlinkMagicBufferHeader hdr = tDataObj.getHdr();
		handler.getInputChannelForId(hdr.getReceiverId()).onBuffer(((Buffer) tDataObj.gettData()), hdr.getSequenceNumber());
	}

	private void handleEvent(InputChannelID evUuid, int seqNum, int size, long buf, int len) {
		if (size == 0) {
			handler.getInputChannelForId(evUuid).onEmptyBuffer(seqNum);
			return;
		}
		MemorySegment memSeg = MemorySegmentFactory.wrap(DiffingoObj.copyBytesToArray(buf, len));
		Buffer buffer = new Buffer(memSeg, FreeingBufferRecycler.INSTANCE, false);
		handler.getInputChannelForId(evUuid).onBuffer(buffer, seqNum);
	}

	private void handleBuffer(InputChannelID uuid, int seqNum, int size, long buf, int len) {
		if (size == 0) {
			handler.getInputChannelForId(uuid).onEmptyBuffer(seqNum);
			return;
		}
		handler.getInputChannelForId(uuid).onBuffer(new KernelMagicBuffer(buf, len), seqNum);
	}

	private void handleHeader(FlinkMagicBufferHeader header) {
		if (header.getSize() == 0) {
			handler.getInputChannelForId(header.getReceiverId()).onEmptyBuffer(header.getSequenceNumber());
		}
	}

	private void handleError(long buf, int len) {
		UnsafeInputStream in = new UnsafeInputStream(buf, len);
		try (ObjectInputStream ois = new ObjectInputStream(in)) {
			Object obj = ois.readObject();

			if (!(obj instanceof Throwable)) {
				throw new ClassCastException("Read object expected to be of type Throwable, " +
					"actual type is " + obj.getClass() + ".");
			}
			Throwable cause = (Throwable) obj;
			InputChannelID receiverId = null;
			if (in.read() != 0) {
				long lower = DiffingoObj.get8NoEndian(in.getCurrentPos());
				long upper = DiffingoObj.get8NoEndian(in.getOffsetChecked(8));
				receiverId = new InputChannelID(lower, upper);
			}
			if (receiverId == null) { /* Fatal error */
				handler.notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
					"Fatal error at remote task manager '" + socket.getAddress() + "'.",
					socket.getAddress(), cause));
			} else {
				RemoteInputChannel inputChannel = handler.getInputChannelForId(receiverId);
				if (cause.getClass() == PartitionNotFoundException.class) {
					inputChannel.onFailedPartitionRequest();
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

}
