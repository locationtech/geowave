package mil.nga.giat.geowave.accumulo.query;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.util.CloseableIteratorWrapper;
import mil.nga.giat.geowave.accumulo.util.CloseableIteratorWrapper.ScannerClosableWrapper;
import mil.nga.giat.geowave.accumulo.util.EntryIteratorWrapper;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.ScanCallback;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;

/**
 * Represents a query operation by an Accumulo row. This abstraction is
 * re-usable for both exact row ID queries and row prefix queries.
 * 
 */
abstract public class AbstractAccumuloRowQuery<T> extends
		AccumuloQuery
{
	protected final ByteArrayId row;
	protected final ScanCallback<T> scanCallback;

	public AbstractAccumuloRowQuery(
			final Index index,
			final ByteArrayId row,
			final String[] authorizations,
			final ScanCallback<T> scanCallback ) {
		super(
				index);
		this.row = row;
		this.scanCallback = scanCallback;
	}

	public T query(
			final AccumuloOperations accumuloOperations,
			final AdapterStore adapterStore ) {
		final ScannerBase scanner = getScanner(
				accumuloOperations,
				getScannerLimit());
		addScanIteratorSettings(scanner);
		final CloseableIteratorWrapper<Object> it = new CloseableIteratorWrapper<Object>(
				new ScannerClosableWrapper(
						scanner),
				new EntryIteratorWrapper(
						adapterStore,
						index,
						scanner.iterator(),
						null));
		return queryResultFromIterator(it);
	}

	protected void addScanIteratorSettings(
			final ScannerBase scanner ) {
		// we have to at least use a whole row iterator
		final IteratorSetting iteratorSettings = new IteratorSetting(
				QueryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY,
				QueryFilterIterator.WHOLE_ROW_ITERATOR_NAME,
				WholeRowIterator.class);
		scanner.addScanIterator(iteratorSettings);
	}

	abstract protected T queryResultFromIterator(
			final CloseableIteratorWrapper<?> it );

	abstract protected Integer getScannerLimit();
}
