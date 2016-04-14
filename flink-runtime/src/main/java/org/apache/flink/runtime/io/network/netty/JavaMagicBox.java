package org.apache.flink.runtime.io.network.netty;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by winston on 13/04/2016.
 */
public class JavaMagicBox implements MagicBox {

	@Override
	public MagicSocket connect(InetSocketAddress address, MagicTypeDesc type) throws IOException {
		return new JavaMagicSocket(address, ((FlinkMagicTypeDesc) type));
	}

}
