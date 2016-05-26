package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;

/**
 * Created by winston on 24/05/2016.
 */
public class HandrolledInputFormat<OT, T extends InputSplit> implements InputFormat<OT, T> {

	private String currentFilePath;
	private long diffingo_file;
	private boolean last;
	private boolean finished;

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
			diffingo_file = DiffingoFile.construct(path, 64, 1, 64);
			currentFilePath = path;
		}
		DiffingoFile.open_split(diffingo_file, fsplit.getStart(), fsplit.getLength());
		last = false;
		finished = false;
	}

	private void deleteAndReset() {
		DiffingoFile.delete(diffingo_file);
		currentFilePath = null;
		diffingo_file = 0;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return finished;
	}

	int[] f0 = new int[1];
	long[] f1 = new long[1];
	char[] str = new char[4096];
	int[] len = new int[1];

	@Override
	public OT nextRecord(OT reuse) throws IOException {
		if (last) {
			finished = true;
			return null;
		}
//		last = DiffingoFile.do_handrolled_read(diffingo_file);
//		last = DiffingoFile.do_critical_read(diffingo_file, f0, f1, str);
		last = DiffingoFile.do_critical_read_line(diffingo_file, str, len);
//		Tuple3<Integer, Long, String> tuple = ((Tuple3<Integer, Long, String>) reuse);
//		tuple.f0 = f0[0];
//		tuple.f1 = f1[0];
//		tuple.f2 = new String(str);
		return (OT) new String(str, 0, len[0]);
//		DiffingoFile.readInto(diffingo_file, (Tuple3<Integer, Long, String>) reuse);
//		return reuse;
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
