package mil.nga.giat.geowave.datastore.cassandra.query;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.operations.BatchedRangeRead;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.operations.RowRead;

/**
 * This class is used internally to perform query operations against an
 * Cassandra data store. The query is defined by the set of parameters passed
 * into the constructor.
 */
abstract public class CassandraQuery
{
	private final static Logger LOGGER = Logger.getLogger(
			CassandraQuery.class);
	protected final List<ByteArrayId> adapterIds;
	protected final PrimaryIndex index;
	protected final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair;
	protected final DifferingFieldVisibilityEntryCount visibilityCounts;
	final CassandraOperations cassandraOperations;

	private final String[] authorizations;

	public CassandraQuery(
			final CassandraOperations cassandraOperations,
			final PrimaryIndex index,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		this(
				cassandraOperations,
				null,
				index,
				null,
				visibilityCounts,
				authorizations);
	}

	public CassandraQuery(
			final CassandraOperations cassandraOperations,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		this.cassandraOperations = cassandraOperations;
		this.adapterIds = adapterIds;
		this.index = index;
		this.fieldIdsAdapterPair = fieldIdsAdapterPair;
		this.visibilityCounts = visibilityCounts;
		this.authorizations = authorizations;
	}

	abstract protected List<ByteArrayRange> getRanges();

	protected boolean isAggregation() {
		return false;
	}

	protected boolean useWholeRowIterator() {
		return (visibilityCounts == null) || visibilityCounts.isAnyEntryDifferingFieldVisiblity();
	}

	protected Iterator<CassandraRow> getResults(
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		final List<ByteArrayRange> ranges = getRanges();
		final String tableName = StringUtils.stringFromBinary(
				index.getId().getBytes());
		if (ranges != null) {
			if (ranges.size() == 1) {
				final ByteArrayRange r = ranges.get(
						0);
				if (r.isSingleValue()) {
					final RowRead rowRead = cassandraOperations.getRowRead(
							tableName);
					rowRead.setRow(
							r.getStart().getBytes());
					return Iterators.singletonIterator(
							rowRead.result());
				}
				else {
					final BatchedRangeRead rangeRead = cassandraOperations.getBatchedRangeRead(
							tableName);
					rangeRead.addQueryRange(
							r);
					return rangeRead.results();
				}
			}
			final BatchedRangeRead rangeRead = cassandraOperations.getBatchedRangeRead(
					tableName,
					ranges);
			return rangeRead.results();
		}
		return EmptyIterator.INSTANCE;
	}

	public String[] getAdditionalAuthorizations() {
		return authorizations;
	}
}
