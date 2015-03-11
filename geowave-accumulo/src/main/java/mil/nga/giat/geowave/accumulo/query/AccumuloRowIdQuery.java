package mil.nga.giat.geowave.accumulo.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.accumulo.util.CloseableIteratorWrapper;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.store.ScanCallback;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.log4j.Logger;

/**
 * Represents a query operation for a specific Accumulo row ID.
 * 
 */
public class AccumuloRowIdQuery extends
		AbstractAccumuloRowQuery<Object>
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloRowIdQuery.class);

	public AccumuloRowIdQuery(
			final Index index,
			final ByteArrayId row,
			final String... authorizations ) {
		super(
				index,
				row,
				authorizations,
				null);
	}

	public AccumuloRowIdQuery(
			final Index index,
			final ByteArrayId row,
			final ScanCallback<Object> scanCallback,
			final String... authorizations ) {
		super(
				index,
				row,
				authorizations,
				scanCallback);
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		ranges.add(new ByteArrayRange(
				row,
				row,
				true));
		return ranges;
	}

	@Override
	protected Integer getScannerLimit() {
		return 1;
	}

	@Override
	protected Object queryResultFromIterator(
			final CloseableIteratorWrapper<?> it ) {

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
}