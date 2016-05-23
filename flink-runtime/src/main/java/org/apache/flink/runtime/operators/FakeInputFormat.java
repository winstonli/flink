package org.apache.flink.runtime.operators;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Scanner;

/**
 * Created by winston on 22/05/2016.
 */
public class FakeInputFormat<OT, T extends InputSplit> implements InputFormat<OT, T> {
	private Scanner scanner;

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
		FileInputSplit fileSplit = ((FileInputSplit) split);
		final FileSystem fs = FileSystem.get(fileSplit.getPath().toUri());
		FSDataInputStream fdis = fs.open(fileSplit.getPath());
		fdis.seek(fileSplit.getStart());
		long total = 0L;
		long stop = fileSplit.getLength() + 1006;
		long remaining = stop;
		byte[] chars1006 = new byte[(int) stop];
		while (total < stop) {
			int read = fdis.read(chars1006, (int) total, (int) remaining);
			if (read == -1) {
				stop = total;
				remaining = 0;
				break;
			}
			total += read;
			remaining -= read;
		}
		for (int i = (int) (stop); i > 0; --i) {
			if (chars1006[i - 1] == '\n') {
				stop = i;
				break;
			}
		}
//		Deque<Tuple2<Integer, String>> mine = new ArrayDeque<>();
		scanner = new Scanner(new ByteArrayInputStream(chars1006, 0, (int) stop));
		if (fileSplit.getStart() > 0 && scanner.hasNextLine()) {
			scanner.nextLine();
		}
//		while (scanner.hasNextLine()) {

//			mine.add(new Tuple2<>(first, second));
//		}
	}

	@Override
	public boolean reachedEnd() throws IOException {
		if (scanner == null) {
			return true;
		}
		boolean ret = !scanner.hasNextLine();
		if (ret) {
			scanner = null;
		}
		return ret;
	}

	@Override
	public OT nextRecord(OT reuse) throws IOException {
		String line = scanner.nextLine();
		String[] splt = StringUtils.split(line, ",", 2);
		int first;
		try {
			first = Integer.parseInt(splt[0]);
		} catch (NumberFormatException e) {
			throw e;
		}
		String second = splt[1];
		return (OT) new Tuple2<>(first, second);
//			output.collect((OT) new Tuple2<>(first, second));
	}

	@Override
	public void close() throws IOException {

	}

}
