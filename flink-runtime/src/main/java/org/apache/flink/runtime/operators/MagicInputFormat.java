package org.apache.flink.runtime.operators;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.io.network.netty.DiffingoObj;

import java.io.IOException;

/**
 * Created by winston on 23/05/2016.
 */
public class MagicInputFormat<OT, T extends InputSplit> implements InputFormat<OT, T> {

	private final int bufSize;
	private final int batchSize;
	private final int stackSize;

	private String currentFilePath;
	private long diffingo_file;

	private long current_rec_ptr_ptr;
	private int remaining;

	public MagicInputFormat(int bufSize, int batchSize, int stackSize) {
		this.bufSize = bufSize;
		this.batchSize = batchSize;
		this.stackSize = stackSize;
		currentFilePath = null;
		diffingo_file = 0;
		current_rec_ptr_ptr = 0;
		remaining = 0;
	}

	@Override
	public void configure(Configuration parameters) {
		throw new UnsupportedOperationException();
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public T[] createInputSplits(int minNumSplits) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(T[] inputSplits) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void open(T split) throws IOException {
		FileInputSplit fsplit = ((FileInputSplit) split);
		String path = fsplit.getPath().toUri().getPath();
		if (currentFilePath != null && !currentFilePath.equals(path)) {
			deleteAndReset();
		}
		if (diffingo_file == 0) {
			diffingo_file = DiffingoFile.construct(path, bufSize, batchSize, stackSize);
			currentFilePath = path;
		}
		DiffingoFile.open_split(diffingo_file, fsplit.getStart(), fsplit.getLength());
	}

	private void deleteAndReset() {
		DiffingoFile.delete(diffingo_file);
		currentFilePath = null;
		diffingo_file = 0;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return remaining == -1;
	}

	private void readNextBatch() {
		DiffingoFile.do_read(diffingo_file);
		this.remaining = DiffingoFile.arr_res_size(diffingo_file);
		current_rec_ptr_ptr = DiffingoFile.arr_res(diffingo_file);
	}

	@Override
	public OT nextRecord(OT reuse) throws IOException {
		if (remaining == 0) {
			readNextBatch();
			if (remaining == 0) {
                remaining = -1;
                return null;
			}
		}
		Preconditions.checkState(remaining > 0);
		long nextPtr = currentAndAdvance();
		DiffingoObj3.readInto(DiffingoObj.unsafe.getAddress(nextPtr), ((Tuple3<Integer, Long, String>) reuse));
		return reuse;
	}

	private long currentAndAdvance() {
		long ret = current_rec_ptr_ptr;
		current_rec_ptr_ptr += 8;
		--remaining;
		return ret;
	}

	@Override
	public void close() throws IOException {
		if (diffingo_file != 0) {
			DiffingoFile.delete(diffingo_file);
			diffingo_file = 0;
			currentFilePath = null;
		}
	}

}
