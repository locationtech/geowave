package mil.nga.giat.geowave.accumulo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.accumulo.CloseableIteratorWrapper.ScannerClosableWrapper;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.log4j.Logger;

/**
 * Represents a query operation for a specific Accumulo row ID.
 * 
 */
public class AccumuloRowIdQuery extends
		AccumuloQuery
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloRowIdQuery.class);
	private final ByteArrayId rowId;

	public AccumuloRowIdQuery(
			final Index index,
			final ByteArrayId rowId ) {
		super(
				index);
		this.rowId = rowId;
	}

	public Object query(
			final AccumuloOperations accumuloOperations,
			final AdapterStore adapterStore ) {
		final ScannerBase scanner = getScanner(
				accumuloOperations,
				1);
		addScanIteratorSettings(scanner);
		final CloseableIteratorWrapper<Object> it = new CloseableIteratorWrapper<Object>(
				new ScannerClosableWrapper(
						scanner),
				new EntryIteratorWrapper(
						adapterStore,
						index,
						scanner.iterator(),
						null));
		Object retVal = null;
		if (it.hasNext()) {
			retVal = it.next();
		}
		try {
			it.close();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to close single row ID query iterator",
					e);
		}
		return retVal;
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

	@Override
	protected List<ByteArrayRange> getRanges() {
		final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		ranges.add(new ByteArrayRange(
				rowId,
				rowId));
		return ranges;
	}
}
