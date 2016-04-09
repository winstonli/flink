package org.apache.flink.core.memory;

/**
 * Created by winston on 05/04/2016.
 */
public interface MagicInputView<T> extends DataInputView {

	T read();

}
