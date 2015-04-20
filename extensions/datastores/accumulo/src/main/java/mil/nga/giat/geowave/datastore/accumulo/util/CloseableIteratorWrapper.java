package mil.nga.giat.geowave.datastore.accumulo.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import mil.nga.giat.geowave.core.store.CloseableIterator;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.log4j.Logger;

/**
 * This is a simple wrapper around the batch scanner's default iterator to
 * ensure that the caller can close the scanner when it is finished being used.
 * 
 * @param <E>
 *            The type to iterate on
 */
public class CloseableIteratorWrapper<E> implements
		CloseableIterator<E>
{
	private final static Logger LOGGER = Logger.getLogger(CloseableIteratorWrapper.class);

	public static class ScannerClosableWrapper implements
			Closeable
	{
		private final ScannerBase scanner;

		public ScannerClosableWrapper(
				final ScannerBase scanner ) {
			this.scanner = scanner;
		}

		@Override
		public void close() {
			scanner.close();
		}

	}

	private final Closeable closeable;
	private final Iterator<E> iterator;

	public CloseableIteratorWrapper(
			final Closeable closable,
			final Iterator<E> iterator ) {
		this.closeable = closable;
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
	protected void finalize()
			throws Throwable {
		super.finalize();
		closeable.close();
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
