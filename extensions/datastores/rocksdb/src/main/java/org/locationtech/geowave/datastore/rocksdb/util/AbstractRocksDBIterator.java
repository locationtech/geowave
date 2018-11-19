package org.locationtech.geowave.datastore.rocksdb.util;

import java.util.NoSuchElementException;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

abstract public class AbstractRocksDBIterator<T> implements
		CloseableIterator<T>
{
	protected boolean closed = false;
	protected ReadOptions options;
	protected RocksIterator it;

	public AbstractRocksDBIterator(
			final ReadOptions options,
			final RocksIterator it ) {
		super();
		this.options = options;
		this.it = it;
	}

	@Override
	public boolean hasNext() {
		return !closed && it.isValid();
	}

	@Override
	public T next() {
		if (closed) {
			throw new NoSuchElementException();
		}
		T retVal = readRow(
				it.key(),
				it.value());

		it.next();
		return retVal;
	}

	abstract protected T readRow(
			byte[] key,
			byte[] value );

	@Override
	public void close() {
		closed = true;
		if (it != null) {
			it.close();
			it = null;
		}
		if (options != null) {
			options.close();
			options = null;
		}
	}
}
