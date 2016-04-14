package org.apache.flink.runtime.io.network.netty;

/**
 * Created by winston on 13/04/2016.
 */
public class FlinkMagicObject {

	private final Object tData;
	private final FlinkMagicBufferHeader hdr;

	public FlinkMagicObject(Object tData, FlinkMagicBufferHeader hdr) {
		this.tData = tData;
		this.hdr = hdr;
	}

	public Object gettData() {
		return tData;
	}

	public FlinkMagicBufferHeader getHdr() {
		return hdr;
	}

}
