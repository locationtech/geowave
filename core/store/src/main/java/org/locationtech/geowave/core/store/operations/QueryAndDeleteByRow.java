package org.locationtech.geowave.core.store.operations;

import java.util.NoSuchElementException;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class QueryAndDeleteByRow<T> implements
		Deleter<T>
{
	private final RowDeleter rowDeleter;
	private final Reader<T> reader;

	public QueryAndDeleteByRow() {
		this.reader = new EmptyReader<>();
		rowDeleter = null;
	}

	public QueryAndDeleteByRow(
			final RowDeleter rowDeleter,
			final Reader<T> reader ) {
		this.rowDeleter = rowDeleter;
		this.reader = reader;
	}

	@Override
	public void close()
			throws Exception {
		reader.close();
		rowDeleter.close();
	}

	@Override
	public boolean hasNext() {
		return reader.hasNext();
	}

	@Override
	public T next() {
		return reader.next();
	}

	@Override
	public void entryScanned(
			final T entry,
			final GeoWaveRow row ) {
		rowDeleter.delete(row);
	}

	private static class EmptyReader<T> implements
			Reader<T>
	{

		@Override
		public void close()
				throws Exception {}

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public T next() {
			throw new NoSuchElementException();
		}

	}
}
