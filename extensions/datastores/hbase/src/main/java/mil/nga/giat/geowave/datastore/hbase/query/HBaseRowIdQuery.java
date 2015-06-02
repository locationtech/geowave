/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCloseableIteratorWrapper;

import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> AccumuloRowIdQuery </code>
 */
public class HBaseRowIdQuery extends
		AbstractHBaseRowQuery<Object>
{

	private final static Logger LOGGER = Logger.getLogger(HBaseRowIdQuery.class);

	public HBaseRowIdQuery(
			final Index index,
			final ByteArrayId row,
			final String... authorizations ) {
		super(
				index,
				row,
				authorizations,
				null);
	}

	@Override
	protected Integer getScannerLimit() {
		return 1;
	}

	@Override
	protected Object queryResultFromIterator(
			HBaseCloseableIteratorWrapper<?> it ) {
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

	@Override
	protected List<ByteArrayRange> getRanges() {
		final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		ranges.add(new ByteArrayRange(
				row,
				row,
				true));
		return ranges;
	}

}
