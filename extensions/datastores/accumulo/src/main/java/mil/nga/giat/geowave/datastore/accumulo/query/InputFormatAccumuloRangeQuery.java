package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.FilterList;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.util.InputFormatIteratorWrapper;

/**
 * * Represents a query operation for a range of Accumulo row IDs. This class is
 * particularly used by the InputFormat as the iterator that it returns will
 * contain Entry<GeoWaveInputKey, Object> entries rather than just the object.
 * This is so the input format has a way of getting the adapter ID and data ID
 * to define the key.
 */
public class InputFormatAccumuloRangeQuery extends
		AccumuloConstraintsQuery
{
	private final static Logger LOGGER = LoggerFactory.getLogger(InputFormatAccumuloRangeQuery.class);
	private final Range accumuloRange;
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

	public InputFormatAccumuloRangeQuery(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Range accumuloRange,
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
				null,
				queryOptions.getAuthorizations());

		this.accumuloRange = accumuloRange;
		this.isOutputWritable = isOutputWritable;
	}

	@Override
	protected ScannerBase getScanner(
			final AccumuloOperations accumuloOperations,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		final String tableName = index.getId().getString();
		Scanner scanner;
		try {
			scanner = accumuloOperations.createScanner(
					tableName,
					getAdditionalAuthorizations());
			scanner.setRange(accumuloRange);
			if ((adapterIds != null) && !adapterIds.isEmpty()) {
				for (final ByteArrayId adapterId : adapterIds) {
					scanner.fetchColumnFamily(new Text(
							adapterId.getBytes()));
				}
			}
			return scanner;
		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
			return null;
		}
	}

	@Override
	protected Iterator initIterator(
			final AdapterStore adapterStore,
			final ScannerBase scanner ) {
		return new InputFormatIteratorWrapper(
				useWholeRowIterator(),
				adapterStore,
				index,
				scanner.iterator(),
				isOutputWritable,
				clientFilters.isEmpty() ? null : clientFilters.size() == 1 ? clientFilters.get(0)
						: new FilterList<QueryFilter>(
								clientFilters));
	}

}
