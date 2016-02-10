/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;

/**
 * @author viggy Functionality similar to <code> AccumuloRowIdQuery </code>
 */
public class HBaseRowIdsQuery<T> extends
		HBaseConstraintsQuery
{

	private final static Logger LOGGER = Logger.getLogger(
			HBaseRowIdsQuery.class);

	final Collection<ByteArrayId> rows;

	public HBaseRowIdsQuery(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final Collection<ByteArrayId> rows,
			final ScanCallback<T> scanCallback,
			final DedupeFilter dedupFilter,
			final String[] authorizations ) {
		super(
				Collections.<ByteArrayId> emptyList(),
				index,
				(Query) null,
				dedupFilter,
				scanCallback,
				null,
				authorizations);
		this.rows = rows;
	}

	public HBaseRowIdsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Collection<ByteArrayId> rows,
			final ScanCallback<T> scanCallback,
			final DedupeFilter dedupFilter,
			final String[] authorizations ) {
		super(
				adapterIds,
				index,
				(Query) null,
				dedupFilter,
				scanCallback,
				null,
				authorizations);
		this.rows = rows;
	}

//	@Override
//	protected Integer getScannerLimit() {
//		return 1;
//	}
//
//	@Override
//	protected Object queryResultFromIterator(
//			final HBaseCloseableIteratorWrapper<?> it ) {
//		Object retVal = null;
//		if (it.hasNext()) {
//			retVal = it.next();
//		}
//		try {
//			it.close();
//		}
//		catch (final IOException e) {
//			LOGGER.warn(
//					"Unable to close single row ID query iterator",
//					e);
//		}
//		return retVal;
//	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		for (final ByteArrayId row : rows) {
			ranges.add(new ByteArrayRange(
					row,
					row,
					true));
		}
		return ranges;
	}

}
