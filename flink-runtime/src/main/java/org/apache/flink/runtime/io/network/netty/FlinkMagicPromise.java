package org.apache.flink.runtime.io.network.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Created by winston on 10/04/2016.
 */
public class FlinkMagicPromise extends DefaultChannelPromise {

	public FlinkMagicPromise(Channel channel) {
		super(channel);
	}

	@Override
	public ChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
		try {
			((ChannelFutureListener) listener).operationComplete(this);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return this;
	}

}
