/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.netty;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.flink.runtime.io.network.buffer.MagicBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by winston on 13/04/2016.
 */
public class JavaMagicSocket implements MagicSocket {

	private final InetSocketAddress address;

	private final Socket socket;
	private final InputStream inputStream;
	private final OutputStream outputStream;

	private final FlinkMagicParser parser;

	private final BlockingQueue<Object> read;

	public JavaMagicSocket(InetSocketAddress address, final FlinkMagicTypeDesc type) throws IOException {
		this.address = address;
		socket = new Socket(address.getHostName(), address.getPort());
		inputStream = socket.getInputStream();
		outputStream = socket.getOutputStream();
		parser = new FlinkMagicParser();
		read = new LinkedBlockingDeque<>();
		new Thread() {

			PooledByteBufAllocator pool = new PooledByteBufAllocator();

			ByteBuffer header = ByteBuffer.allocate(NettyMessage.HEADER_LENGTH);

//			FileOutputStream fout = new FileOutputStream("/tmp/flink-out.out");

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
//					fout.write(header.array(), 0, NettyMessage.HEADER_LENGTH);
//					fout.flush();
					int frameLen = header.getInt(0);
					int magic = header.getInt(4);
					Preconditions.checkArgument(magic == NettyMessage.MAGIC_NUMBER);
					int id = header.get(8);

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
//						fout.write(bbuf.array(), bbuf.arrayOffset() + bbuf.readerIndex(), bodyLen);
//						fout.flush();

						switch (id) {
						case 0: /* BufferResponse */
							doReadBufferResponse(bbuf);
							break;
						case 1: /* ErrorResponse */
							doReadErrorResponse(bbuf);
							break;
						default:
							throw new IllegalStateException("id was wrong");
						}
					} finally {
						bbuf.release();
					}
				}
//				fout.close();
			}

			private void doReadErrorResponse(ByteBuf bbuf) {
				byte[] err = new byte[bbuf.readableBytes()];
				bbuf.readBytes(err);
				read.add(new FlinkMagicObjectError(err));
				read.notifyAll();
			}

			private void doReadBufferResponse(ByteBuf bbuf) throws IOException {
				InputChannelID receiverId = InputChannelID.fromByteBuf(bbuf);
				int sequenceNumber = bbuf.readInt();
				boolean isBuffer = bbuf.readBoolean();
				int size = bbuf.readInt();
				if (isBuffer) {
					FlinkMagicBufferHeader hdr = new FlinkMagicBufferHeader(receiverId, sequenceNumber, isBuffer, size);
					read.add(hdr);
					MagicBuffer b = parser.parse(bbuf, size, type.getT(), receiverId);
					read.add(new FlinkMagicObject(b, hdr));
				} else {
					byte[] copy = new byte[size];
					bbuf.readBytes(copy);
					read.add(new FlinkMagicBufferEvent(receiverId, sequenceNumber, copy));
				}
			}

		}.start();
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	@Override
	public MagicResult readJ() throws IOException {
		Object res = null;
		try {
			res = read.take();
		} catch (InterruptedException e) {
			throw new IOException();
		}
		return new FlinkMagicResult(res);
	}

	@Override
	public long read() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void write(byte[] data) throws IOException {
		outputStream.write(data);
	}

	@Override
	public void write(byte[] array, int offset, int len) throws IOException {
		outputStream.write(array, offset, len);
	}

	@Override
	public void write(int b) throws IOException {
		outputStream.write(b);
	}

}
