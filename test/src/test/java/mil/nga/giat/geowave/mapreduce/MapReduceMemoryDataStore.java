package mil.nga.giat.geowave.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.memory.MemoryRequiredOptions;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

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
				new MemoryRequiredOptions().getStoreOptions());
	}

	@Override
	public List<InputSplit> getSplits(
			DistributableQuery query,
			QueryOptions queryOptions,
			AdapterStore adapterStore,
			AdapterIndexMappingStore aimStore,
			DataStatisticsStore statsStore,
			IndexStore indexStore,
			Integer minSplits,
			Integer maxSplits )
			throws IOException,
			InterruptedException {
		return super.getSplits(
				query,
				queryOptions,
				this.adapterStore,
				this.indexMappingStore,
				this.statisticsStore,
				this.indexStore,
				minSplits,
				maxSplits);
	}

}
