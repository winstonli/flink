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

import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultChannelPromise;
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
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Created by winston on 10/04/2016.
 */
public class FlinkMagicClient {

	private final PartitionRequestClientHandler handler;

	private final MagicBox magic;

	public FlinkMagicClient(PartitionRequestClientHandler handler) {
		this.handler = handler;
		magic = new KernelMagicBox();
	}

	public ChannelFuture connect(final InetSocketAddress address, MagicTypeDesc type) {
		KernelMagicChannel channel = new KernelMagicChannel(handler);
		channel.setSocket(magic.connectWithHandler(address, type, new MagicHandler() {

			@Override
			public void handleError(long buf, int len) {
				System.out.println("handleError(" + buf + ", " + len + ") called");
				handleErr(address, buf, len);
			}

			@Override
			public void handleEvent(InputChannelID uuid, int seqNum, int size, long buf, int len) {
				System.out.println("handleEvent(" + uuid + ", " + seqNum + ", " + size + ", " + buf + ", " + len + ") called");
				handleEv(uuid, seqNum, size, buf, len);
			}

			@Override
			public void handleBuffer(InputChannelID uuid, int seqNum, int size, long buf, int len, long diffObj) {
				if (size == 0) {
					handler.getInputChannelForId(uuid).onEmptyBuffer(seqNum);
					return;
				}
				handler.getInputChannelForId(uuid).onBuffer(new KernelMagicBuffer(diffObj, buf, len), seqNum);
			}

			@Override
			public void handleHeader() {
			}

		}));
		DefaultChannelPromise p = new FlinkMagicPromise(channel);
		return p.setSuccess();
	}

	private void handleErr(SocketAddress addr, long buf, int len) {
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
					"Fatal error at remote task manager '" + addr + "'.",
					addr, cause));
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

	private void handleEv(InputChannelID evUuid, int seqNum, int size, long buf, int len) {
		if (size == 0) {
			handler.getInputChannelForId(evUuid).onEmptyBuffer(seqNum);
			return;
		}
		MemorySegment memSeg = MemorySegmentFactory.wrap(DiffingoObj.copyBytesToArray(buf, len));
		Buffer buffer = new Buffer(memSeg, FreeingBufferRecycler.INSTANCE, false);
		handler.getInputChannelForId(evUuid).onBuffer(buffer, seqNum);
	}

}
