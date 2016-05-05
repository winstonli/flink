package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

/**
 * Created by winston on 02/05/2016.
 */
public interface MagicHandler {

	void handleHeader();

	void handleBuffer(InputChannelID uuid, int seqNum, int size, Object[] parsed);

	void handleEvent(InputChannelID uuid, int seqNum, int size, long buf, int len);

	void handleError(long buf, int len);

}
