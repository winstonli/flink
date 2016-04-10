package org.apache.flink.runtime.io.network.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultChannelPromise;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Created by winston on 10/04/2016.
 */
public class MagicClient {

	private final PartitionRequestClientHandler handler;

	public MagicClient(PartitionRequestClientHandler handler) {
		this.handler = handler;
	}

	public ChannelFuture connect(InetSocketAddress serverSocketAddress) {
		MagicChannel channel = new MagicChannel(handler);
		try {
			Socket socket = new Socket(serverSocketAddress.getHostName(), serverSocketAddress.getPort());
			channel.setSocket(socket);
			DefaultChannelPromise p = new MagicPromise(channel);
			return p.setSuccess();
		} catch (IOException e) {
			e.printStackTrace();
			return new DefaultChannelPromise(channel).setFailure(e);
		}
	}

}
