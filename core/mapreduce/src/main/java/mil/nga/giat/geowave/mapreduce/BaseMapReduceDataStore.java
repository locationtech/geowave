package mil.nga.giat.geowave.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRecordReader;

public abstract class BaseMapReduceDataStore extends
		BaseDataStore implements
		MapReduceDataStore
{

	public BaseMapReduceDataStore(
			IndexStore indexStore,
			AdapterStore adapterStore,
			DataStatisticsStore statisticsStore,
			AdapterIndexMappingStore indexMappingStore,
			SecondaryIndexDataStore secondaryIndexDataStore,
			MapReduceDataStoreOperations operations,
			DataStoreOptions options ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				operations,
				options);
	}

	@Override
	public RecordReader<GeoWaveInputKey, ?> createRecordReader(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final boolean isOutputWritable,
			final InputSplit inputSplit )
			throws IOException,
			InterruptedException {
		return new GeoWaveRecordReader(
				query,
				queryOptions,
				isOutputWritable,
				adapterStore,
				(MapReduceDataStoreOperations) baseOperations);
	}
}
