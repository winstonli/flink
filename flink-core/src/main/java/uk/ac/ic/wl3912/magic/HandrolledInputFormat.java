package uk.ac.ic.wl3912.magic;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by winston on 24/05/2016.
 */
public class HandrolledInputFormat<OT, T extends InputSplit> implements InputFormat<OT, T> {

	private String currentFilePath = null;
	private long csv_file_parser_ptr = 0;
	private csv_file_parser p;
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
		if (csv_file_parser_ptr == 0) {
			csv_file_parser_ptr = csv_file_parser.create(path);
			currentFilePath = path;
			try {
				p = Mycsv_file_parser.class.getDeclaredConstructor(long.class).newInstance(csv_file_parser_ptr);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
		}
		csv_file_parser.open_split(
                csv_file_parser_ptr,
                fsplit.getStart(),
                fsplit.getLength()
		);
		last = false;
		finished = false;
	}

	private void deleteAndReset() {
		csv_file_parser.delete(csv_file_parser_ptr);
		currentFilePath = null;
		csv_file_parser_ptr = 0;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return finished;
	}
//
//	int[] f0 = new int[1];
//	long[] f1 = new long[1];
//	char[] str = new char[4096];
//	int[] len = new int[1];
//

	char[] url = new char[2048];
	int[] url_len_ptr = new int[1];
	char[] link = new char[3072];
	int[] link_len_ptr = new int[1];

	@Override
	public OT nextRecord(OT reuse) throws IOException {
		if (last) {
			finished = true;
			return null;
		}
//		last = DiffingoFile.do_handrolled_read(diffingo_file);
//		last = DiffingoFile.do_critical_read(diffingo_file, f0, f1, str);
		last = p.read(url, url_len_ptr, link, link_len_ptr);
//		last = csv_file_parser.read(
//                csv_file_parser_ptr,
//                url,
//                url_len_ptr,
//                link,
//                link_len_ptr
//		);
//		Tuple3<Integer, Long, String> tuple = ((Tuple3<Integer, Long, String>) reuse);
//		tuple.f0 = f0[0];
//		tuple.f1 = f1[0];
//		tuple.f2 = new String(str);
		return (OT)  new Tuple2<>(
			new String(url, 0, url_len_ptr[0]),
			new String(link, 0, link_len_ptr[0])
		);
//		DiffingoFile.readInto(diffingo_file, (Tuple3<Integer, Long, String>) reuse);
//		return reuse;
	}
//
	@Override
	public void close() throws IOException {
		if (csv_file_parser_ptr != 0) {
			csv_file_parser.delete(csv_file_parser_ptr);
			csv_file_parser_ptr = 0;
			currentFilePath = null;
		}
	}

	private static class Mycsv_file_parser extends csv_file_parser {

		private final long ptr;

		public Mycsv_file_parser(long ptr) {
			this.ptr = ptr;
		}

		@Override
        public void open_split(long offset, long len) {
            csv_file_parser.open_split(ptr, offset, len);
        }

		@Override
        public boolean read(char[] url, int[] url_len_ptr, char[] link, int[] link_len_ptr) {
            return csv_file_parser.read(ptr, url, url_len_ptr, link, link_len_ptr);
        }
	}
}
