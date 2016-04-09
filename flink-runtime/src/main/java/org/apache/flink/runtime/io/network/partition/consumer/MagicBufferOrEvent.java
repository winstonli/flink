package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.buffer.MagicBuffer;

/**
 * Created by winston on 05/04/2016.
 */
public class MagicBufferOrEvent<T extends IOReadableWritable> extends BufferOrEvent {

	public MagicBufferOrEvent(MagicBuffer<T> buffer, int channelIndex) {
		super(buffer, channelIndex);
	}

}
