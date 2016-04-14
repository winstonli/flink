package org.apache.flink.runtime.io.network.netty;

/**
 * Created by winston on 13/04/2016.
 */
public class FlinkMagicObjectError {

	private final byte[] err;

	public FlinkMagicObjectError(byte[] err) {
		this.err = err;
	}

	public byte[] getErr() {
		return err;
	}

}
