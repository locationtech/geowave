/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.core.store.CloseableIterator;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to
 *         <code> CloseableIteratorWrapper </code>
 */
public class HBaseCloseableIteratorWrapper<E> implements
		CloseableIterator<E>
{

	private final static Logger LOGGER = Logger.getLogger(HBaseCloseableIteratorWrapper.class);

	public static class ScannerClosableWrapper implements
			Closeable
	{
		private final ResultScanner results;

		public ScannerClosableWrapper(
				final ResultScanner results ) {
			this.results = results;
		}

		@Override
		public void close() {
			results.close();
		}

	}

	public static class MultiScannerClosableWrapper implements
			Closeable
	{
		private final List<ResultScanner> results;

		public MultiScannerClosableWrapper(
				final List<ResultScanner> results ) {
			this.results = results;
		}

		@Override
		public void close() {
			for(ResultScanner scanner : results){
				scanner.close();
			}
		}

	}

	private final Closeable closeable;
	private final Iterator<E> iterator;

	public HBaseCloseableIteratorWrapper(
			Closeable closeable,
			Iterator<E> iterator ) {
		this.closeable = closeable;
		this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
		final boolean hasNext = iterator.hasNext();
		if (!hasNext) {
			try {
				closeable.close();
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to close iterator",
						e);
			}
		}
		return hasNext;
	}

	@Override
	public E next() {
		return iterator.next();
	}

	@Override
	public void remove() {
		iterator.remove();
	}

	@Override
	public void close()
			throws IOException {
		try {
			closeable.close();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to close iterator",
					e);
		}
	}

}
