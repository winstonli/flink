package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.core.io.IOReadableWritable;

/**
 * Created by winston on 13/04/2016.
 */
public class FlinkMagicTypeDesc implements MagicTypeDesc {

	private final IOReadableWritable t;

	public FlinkMagicTypeDesc(IOReadableWritable t) {
		this.t = t;
	}

	static MagicTypeDesc ofIOReadableWritable(IOReadableWritable t) {
		return new FlinkMagicTypeDesc(t);
	}

	public IOReadableWritable getT() {
		return t;
	}
}
