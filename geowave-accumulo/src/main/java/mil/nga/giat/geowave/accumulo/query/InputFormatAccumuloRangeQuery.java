package mil.nga.giat.geowave.accumulo.query;

import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.util.InputFormatIteratorWrapper;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.filter.FilterList;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

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
	private final static Logger LOGGER = Logger.getLogger(InputFormatAccumuloRangeQuery.class);
	private final Range accumuloRange;

	public InputFormatAccumuloRangeQuery(
			final List<ByteArrayId> adapterIds,
			final Index index,
			final Range accumuloRange,
			final List<QueryFilter> queryFilters,
			final String[] authorizations ) {
		super(
				adapterIds,
				index,
				null,
				queryFilters,
				authorizations);
		this.accumuloRange = accumuloRange;
	}

	@Override
	protected ScannerBase getScanner(
			final AccumuloOperations accumuloOperations,
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
				adapterStore,
				index,
				scanner.iterator(),
				new FilterList<QueryFilter>(
						clientFilters));
	}

}
