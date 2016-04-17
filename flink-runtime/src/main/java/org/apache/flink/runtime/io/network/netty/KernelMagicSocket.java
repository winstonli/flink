package org.apache.flink.runtime.io.network.netty;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by winston on 14/04/2016.
 */
public class KernelMagicSocket implements MagicSocket {

	private final InetSocketAddress address;
	private final MagicTypeDesc type;
	private boolean connected;
	private long kmagic_socket;

	public KernelMagicSocket(InetSocketAddress address, MagicTypeDesc type) {
		this.address = address;
		this.type = type;
		connected = false;
		kmagic_socket = new_kmagic_socket();
	}

	@Override
	protected void finalize() throws Throwable {
		delete(kmagic_socket);
		super.finalize();
	}

	private static native long new_kmagic_socket();

	private static native void delete(long kmagic_socket);

	@Override
	public InetSocketAddress getAddress() {
		return address;
	}

	@Override
	public MagicResult readJ() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long read() {
		return doRead(kmagic_socket);
	}

	private static native long doRead(long kmagic_socket);

	@Override
	public void write(byte[] data) throws IOException {
		write(data, 0, data.length);
	}

	@Override
	public void write(byte[] array, int offset, int len) {
		doWrite(kmagic_socket, array, offset, len);
	}

	private static native int doWrite(long kmagic_socket, byte[] array, int offset, int len);

	@Override
	public void write(int b) {
		doWrite(kmagic_socket, b);
	}

	private static native int doWrite(long kmagic_socket, int b);

	private static native void doConnect(long kmagic_socket, byte[] address, int port);

	public void connect() {
		Preconditions.checkState(!connected);
		connected = true;
		doConnect(kmagic_socket, address.getAddress().getAddress(), address.getPort());
	}

}
