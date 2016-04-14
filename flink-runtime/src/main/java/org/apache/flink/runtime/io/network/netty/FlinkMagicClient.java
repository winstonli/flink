package org.apache.flink.runtime.io.network.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultChannelPromise;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by winston on 10/04/2016.
 */
public class FlinkMagicClient {

	private final PartitionRequestClientHandler handler;

	private final MagicBox magic;

	public FlinkMagicClient(PartitionRequestClientHandler handler) {
		this.handler = handler;
		magic = new JavaMagicBox();
	}

	public ChannelFuture connect(InetSocketAddress serverSocketAddress, MagicTypeDesc type) {
		FlinkMagicChannel channel = new FlinkMagicChannel(handler);
		try {
			channel.setSocket(magic.connect(serverSocketAddress, type));
			DefaultChannelPromise p = new FlinkMagicPromise(channel);
			return p.setSuccess();
		} catch (IOException e) {
			e.printStackTrace();
			return new DefaultChannelPromise(channel).setFailure(e);
		}
	}

}
