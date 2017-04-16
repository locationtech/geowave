package mil.nga.giat.geowave.datastore.cassandra.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.query.InputFormatCassandraRangeQuery;
import mil.nga.giat.geowave.datastore.cassandra.split.CassandraSplitsProvider;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRecordReader;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;

public class GeoWaveCassandraRecordReader<T> extends
		GeoWaveRecordReader<T>
{

	// private DynamoDBOperations dynamoDBOperations;
	private CassandraOperations cassandraOperations;

	public GeoWaveCassandraRecordReader(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final boolean isOutputWritable,
			final AdapterStore adapterStore,
			final BaseDataStore dataStore,
			final CassandraOperations cassandraOperations ) {
		super(
				query,
				queryOptions,
				isOutputWritable,
				adapterStore,
				dataStore);
		this.cassandraOperations = cassandraOperations;
	}

	@Override
	protected CloseableIterator queryRange(
			PrimaryIndex i,
			GeoWaveRowRange range,
			List queryFilters,
			QueryOptions rangeQueryOptions ) {
		// this will take the inputsplit

		return new InputFormatCassandraRangeQuery(
				dataStore,
				adapterStore,
				cassandraOperations,
				i,
				CassandraSplitsProvider.unwrapRange(range),
				queryFilters != null ? queryFilters : new ArrayList<QueryFilter>(), // TODO
																					// aperi:
																					// figure
																					// out
																					// if
																					// this
																					// is
																					// correct
																					// or
																					// not
				isOutputWritable,
				rangeQueryOptions).query(
				adapterStore,
				rangeQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				rangeQueryOptions.getLimit());
	}

	@Override
	public boolean nextKeyValue()
			throws IOException,
			InterruptedException {
		if (iterator != null) {
			if (iterator.hasNext()) {
				++numKeysRead;
				final Object value = iterator.next();
				if (value instanceof Entry) {
					final Entry<GeoWaveInputKey, T> entry = (Entry<GeoWaveInputKey, T>) value;
					currentGeoWaveKey = entry.getKey();
					// TODO implement progress reporting
					// if (currentGeoWaveKey == null) {
					// currentAccumuloKey = null;
					// }
					// else if (currentGeoWaveKey.getInsertionId() != null) {
					// // just use the insertion ID for progress
					// currentAccumuloKey = new Key(
					// new Text(
					// currentGeoWaveKey.getInsertionId().getBytes()));
					// }
					currentValue = entry.getValue();
				}
				return true;
			}
		}
		return false;
	}

	@Override
	public float getProgress()
			throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

}
