package mil.nga.giat.geowave.datastore.dynamodb.mapreduce;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRecordReader;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;

public class GeoWaveDynamoDBRecordReader<T> extends GeoWaveRecordReader<T> {

	private DynamoDBOperations dynamoDBOperations;
	
	public GeoWaveDynamoDBRecordReader(
			final DistributableQuery query, 
			final QueryOptions queryOptions, 
			final boolean isOutputWritable,
			final AdapterStore adapterStore, 
			final BaseDataStore dataStore, 
			final DynamoDBOperations dynamoDBOperations) {
		super(query, queryOptions, isOutputWritable, adapterStore, dataStore);
		this.dynamoDBOperations = dynamoDBOperations;
	}

	@Override
	protected CloseableIterator queryRange(PrimaryIndex i, GeoWaveRowRange range, List queryFilters,
			QueryOptions rangeQueryOptions) {
		//this will take the inputsplit
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

}
