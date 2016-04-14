package org.apache.flink.runtime.io.network.netty;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by winston on 13/04/2016.
 */
public interface MagicBox {

	MagicSocket connect(InetSocketAddress address,
						MagicTypeDesc type) throws IOException;

}
