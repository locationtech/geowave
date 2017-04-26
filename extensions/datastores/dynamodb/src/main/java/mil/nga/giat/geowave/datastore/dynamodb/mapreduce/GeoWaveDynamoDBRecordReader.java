package mil.nga.giat.geowave.datastore.dynamodb.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.datastore.dynamodb.query.InputFormatDynamoDBRangeQuery;
import mil.nga.giat.geowave.datastore.dynamodb.split.DynamoDBSplitsProvider;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRecordReader;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;

public class GeoWaveDynamoDBRecordReader<T> extends
		GeoWaveRecordReader<T>
{

	private DynamoDBOperations dynamoDBOperations;

	public GeoWaveDynamoDBRecordReader(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final boolean isOutputWritable,
			final AdapterStore adapterStore,
			final BaseDataStore dataStore,
			final DynamoDBOperations dynamoDBOperations ) {
		super(
				query,
				queryOptions,
				isOutputWritable,
				adapterStore,
				dataStore);
		this.dynamoDBOperations = dynamoDBOperations;
	}

	@Override
	protected CloseableIterator queryRange(
			final PrimaryIndex i,
			final GeoWaveRowRange range,
			final List<QueryFilter> queryFilters,
			final QueryOptions rangeQueryOptions ) {
		// this will take the inputsplit
		return new InputFormatDynamoDBRangeQuery(
				dataStore,
				adapterStore,
				dynamoDBOperations,
				i,
				DynamoDBSplitsProvider.unwrapRange(range),
				queryFilters,
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
