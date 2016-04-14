package org.apache.flink.runtime.io.network.netty;

/**
 * Created by winston on 13/04/2016.
 */
public class FlinkMagicResult implements MagicResult {

	private final Object tData;

	public FlinkMagicResult(Object tData) {
		this.tData = tData;
	}

	public Object getTData() {
		return tData;
	}

}
