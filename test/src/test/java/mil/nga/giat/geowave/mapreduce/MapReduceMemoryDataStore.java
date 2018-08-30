package mil.nga.giat.geowave.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.TransientAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.memory.MemoryRequiredOptions;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.InternalAdapterStoreImpl;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRecordReader;

public class MapReduceMemoryDataStore extends
		BaseMapReduceDataStore
{
	public MapReduceMemoryDataStore() {
		this(
				new MapReduceMemoryOperations());
	}

	public MapReduceMemoryDataStore(
			final MapReduceDataStoreOperations operations ) {
		super(
				new IndexStoreImpl(
						operations,
						new MemoryRequiredOptions().getStoreOptions()),
				new AdapterStoreImpl(
						operations,
						new MemoryRequiredOptions().getStoreOptions()),
				new DataStatisticsStoreImpl(
						operations,
						new MemoryRequiredOptions().getStoreOptions()),
				new AdapterIndexMappingStoreImpl(
						operations,
						new MemoryRequiredOptions().getStoreOptions()),
				null,
				operations,
				new MemoryRequiredOptions().getStoreOptions(),
				new InternalAdapterStoreImpl(
						operations));
	}

	@Override
	public List<InputSplit> getSplits(
			DistributableQuery query,
			QueryOptions queryOptions,
			TransientAdapterStore adapterStore,
			AdapterIndexMappingStore aimStore,
			DataStatisticsStore statsStore,
			InternalAdapterStore internalAdapterStore,
			IndexStore indexStore,
			Integer minSplits,
			Integer maxSplits )
			throws IOException,
			InterruptedException {
		return super.getSplits(
				query,
				queryOptions,
				adapterStore,
				this.indexMappingStore,
				this.statisticsStore,
				this.internalAdapterStore,
				this.indexStore,
				minSplits,
				maxSplits);
	}

	public PersistentAdapterStore getAdapterStore() {
		return this.adapterStore;
	}
}
