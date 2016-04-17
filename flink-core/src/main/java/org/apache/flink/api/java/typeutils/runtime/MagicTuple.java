package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.netty.Tuple2Record;

/**
 * Created by winston on 17/04/2016.
 */
public class MagicTuple extends Tuple2 {

	public MagicTuple() {
		this(0, -1);
	}

	public MagicTuple(long buf, int len) {
		f0 = Tuple2Record.key(buf);
		f1 = Tuple2Record.copyOfString(buf);
	}



}
