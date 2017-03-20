package mil.nga.giat.geowave.datastore.cassandra.query;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.DataStoreQuery;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.cassandra.CassandraDataStore;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow.CassandraField;
import mil.nga.giat.geowave.datastore.cassandra.operations.BatchedRangeRead;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations.ByteArrayIdToByteBuffer;
import mil.nga.giat.geowave.datastore.cassandra.operations.RowRead;

/**
 * This class is used internally to perform query operations against an
 * Cassandra data store. The query is defined by the set of parameters passed
 * into the constructor.
 */
abstract public class CassandraQuery extends
		DataStoreQuery
{
	private final static Logger LOGGER = Logger.getLogger(CassandraQuery.class);

	final CassandraOperations cassandraOperations;

	public CassandraQuery(
			final BaseDataStore dataStore,
			final CassandraOperations cassandraOperations,
			final PrimaryIndex index,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		this(
				dataStore,
				cassandraOperations,
				null,
				index,
				null,
				visibilityCounts,
				authorizations);
	}

	public CassandraQuery(
			final BaseDataStore dataStore,
			final CassandraOperations cassandraOperations,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		super(
				dataStore,
				adapterIds,
				index,
				fieldIdsAdapterPair,
				visibilityCounts,
				authorizations);

		this.cassandraOperations = cassandraOperations;
	}

	protected CloseableIterator<CassandraRow> getResults(
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		final List<ByteArrayRange> ranges = getRanges();
		final String tableName = StringUtils.stringFromBinary(
				index.getId().getBytes());
		if ((ranges != null) && !ranges.isEmpty()) {
			if ((ranges.size() == 1) && (adapterIds.size() == 1)) {
				final ByteArrayRange r = ranges.get(
						0);
				if (r.isSingleValue()) {
					final RowRead rowRead = cassandraOperations.getRowRead(
							tableName);
					rowRead.setRow(
							r.getStart().getBytes(),
							adapterIds.get(
									0).getBytes());
					return new Wrapper(
							Iterators.singletonIterator(
									rowRead.result()));
				}
				else {
					final BatchedRangeRead rangeRead = cassandraOperations.getBatchedRangeRead(
							tableName,
							adapterIds);
					rangeRead.addQueryRange(
							r);
					return rangeRead.results();
				}
			}
			final BatchedRangeRead rangeRead = cassandraOperations.getBatchedRangeRead(
					tableName,
					adapterIds,
					ranges);
			return rangeRead.results();
		}
		// query everything
		return cassandraOperations.executeQuery(
				cassandraOperations
						.getSelect(
								tableName)
						.where(
								QueryBuilder.in(
										CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
										IntStream
												.range(
														0,
														CassandraDataStore.PARTITIONS)
												.mapToObj(
														i -> ByteBuffer.wrap(
																new byte[] {
																	(byte) i
																})).collect(
																		Collectors.toList())))
						.and(
								QueryBuilder.in(
										CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
										Lists.transform(
												adapterIds,
												new ByteArrayIdToByteBuffer()))));
	}
}
