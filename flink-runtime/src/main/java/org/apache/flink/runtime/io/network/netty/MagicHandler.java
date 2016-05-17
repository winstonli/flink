package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

/**
 * Created by winston on 02/05/2016.
 */
public interface MagicHandler {

	void handleHeader();

	void handleBuffer(InputChannelID uuid, int seqNum, int size, long buf, int len, long diffObj);

	void handleEvent(InputChannelID uuid, int seqNum, int size, long buf, int len);

	void handleError(long buf, int len);

	void handleDiffingoBuffer(long kmagic_socket, InputChannelID uuid, int seqNum, int size, long rec_arr, int arr_len, long msg_resource);

	void handleDiffingoEvent(long kmagic_socket, InputChannelID uuid, int seqNum, int size, long buf, int len, long msg_resource);

	void handleDiffingoError(long kmagic_socket, long buf, int len, long msg_resource);

}
