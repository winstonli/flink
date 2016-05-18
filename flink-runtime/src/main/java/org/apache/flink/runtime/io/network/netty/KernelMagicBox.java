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
		String magicType = System.getProperty("magic");
		if (magicType != null && magicType.equals("diffingo")) {
            KMagicSocket sock = new KMagicSocket(addr, type, 2000, 65534);
            sock.connectDiffingo(handler);
            return sock;
		}
		KernelMagicSocket sock = new KernelMagicSocket(addr, type);
		sock.connectWithHandler(handler);
		return sock;
	}

}
