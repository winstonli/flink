package org.apache.flink.runtime.io.network.netty;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by winston on 14/04/2016.
 */
public class KernelMagicBox implements MagicBox {

	static {
		System.loadLibrary("kmagic_jni");
	}

	@Override
	public MagicSocket connect(InetSocketAddress address, MagicTypeDesc type) throws IOException {
		KernelMagicSocket kernelMagicSocket = new KernelMagicSocket(address, type);
		kernelMagicSocket.connect();
		return kernelMagicSocket;
	}

	@Override
	public MagicSocket connectWithHandler(InetSocketAddress addr, MagicTypeDesc type, MagicHandler handler) {
		KernelMagicSocket sock = new KernelMagicSocket(addr, type);
		sock.connectDiffingo(handler);
		return sock;
	}

}
