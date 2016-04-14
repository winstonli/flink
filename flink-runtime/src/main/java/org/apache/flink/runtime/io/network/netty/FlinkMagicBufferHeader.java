package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

/**
 * Created by winston on 13/04/2016.
 */
public class FlinkMagicBufferHeader {

	private final InputChannelID receiverId;
	private final int sequenceNumber;
	private final boolean isBuffer;
	private final int size;

	public FlinkMagicBufferHeader(InputChannelID receiverId, int sequenceNumber, boolean isBuffer, int size) {
		this.receiverId = receiverId;
		this.sequenceNumber = sequenceNumber;
		this.isBuffer = isBuffer;
		this.size = size;
	}

	public InputChannelID getReceiverId() {
		return receiverId;
	}

	public int getSequenceNumber() {
		return sequenceNumber;
	}

	public boolean isBuffer() {
		return isBuffer;
	}

	public int getSize() {
		return size;
	}

}
