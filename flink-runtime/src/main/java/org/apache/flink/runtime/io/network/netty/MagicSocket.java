package org.apache.flink.runtime.io.network.netty;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by winston on 13/04/2016.
 */
public interface MagicSocket {

	InetSocketAddress getAddress();

	MagicResult read() throws IOException;

	void write(byte[] data) throws IOException;

	void write(byte[] array, int offset, int len) throws IOException;

	void write(int b) throws IOException;

}
