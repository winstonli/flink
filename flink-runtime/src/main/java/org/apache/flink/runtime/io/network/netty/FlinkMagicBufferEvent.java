package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

/**
 * Created by winston on 13/04/2016.
 */
public class FlinkMagicBufferEvent {

	private final InputChannelID receiverId;
	private final int sequenceNumber;
	private final byte[] copy;

	public FlinkMagicBufferEvent(InputChannelID receiverId, int sequenceNumber, byte[] copy) {
		this.receiverId = receiverId;
		this.sequenceNumber = sequenceNumber;
		this.copy = copy;
	}

	public InputChannelID getReceiverId() {
		return receiverId;
	}

	public int getSequenceNumber() {
		return sequenceNumber;
	}

	public byte[] getCopy() {
		return copy;
	}

}
