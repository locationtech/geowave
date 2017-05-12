package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseInputFormatIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

/**
 * * Represents a query operation for a range of HBase row IDs. This class is
 * particularly used by the InputFormat as the iterator that it returns will
 * contain Entry<GeoWaveInputKey, Object> entries rather than just the object.
 * This is so the input format has a way of getting the adapter ID and data ID
 * to define the key.
 */
public class InputFormatHBaseRangeQuery extends
		HBaseConstraintsQuery
{
	private final static Logger LOGGER = LoggerFactory.getLogger(InputFormatHBaseRangeQuery.class);
	private final ByteArrayRange range;
	private final boolean isOutputWritable;

	private static List<ByteArrayId> getAdapterIds(
			final PrimaryIndex index,
			final AdapterStore adapterStore,
			final QueryOptions queryOptions ) {
		try {
			return queryOptions.getAdapterIds(adapterStore);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Adapter IDs not set and unattainable from the AdapterStore",
					e);
		}
		return Collections.emptyList();
	}

	public InputFormatHBaseRangeQuery(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final ByteArrayRange range,
			final List<QueryFilter> queryFilters,
			final boolean isOutputWritable,
			final QueryOptions queryOptions ) {
		super(
				getAdapterIds(
						index,
						adapterStore,
						queryOptions),
				index,
				null,
				queryFilters,
				(DedupeFilter) null,
				queryOptions.getScanCallback(),
				null,
				null,
				null,
				null,
				queryOptions.getAuthorizations());

		this.range = range;
		this.isOutputWritable = isOutputWritable;
	}

	@Override
	protected Iterator initIterator(
			final AdapterStore adapterStore,
			final Iterator<Result> resultsIterator,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean decodePersistenceEncoding ) {
		// TODO Since currently we are not supporting server side
		// iterator/coprocessors, we also cannot run
		// server side filters and hence they have to run on clients itself. So
		// need to add server side filters also in list of client filters.
		final List<QueryFilter> filters = getAllFiltersList();
		return new HBaseInputFormatIteratorWrapper(
				adapterStore,
				index,
				resultsIterator,
				isOutputWritable,
				filters.isEmpty() ? null : filters.size() == 1 ? filters.get(0)
						: new mil.nga.giat.geowave.core.store.filter.FilterList<QueryFilter>(
								filters));
	}

	@Override
	protected Scan getMultiScanner(
			final FilterList filterList,
			final Integer limit,
			final double[] maxResolutionSubsamplingPerDimension ) {
		final Scan scanner = createStandardScanner(limit);

		scanner.setStartRow(range.getStart().getBytes());
		scanner.setStopRow(range.getEnd().getBytes());

		return scanner;
	}
}
