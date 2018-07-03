package mil.nga.giat.geowave.datastore.dynamodb;

import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.InternalAdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.SecondaryIndexStoreImpl;
import mil.nga.giat.geowave.datastore.dynamodb.operations.DynamoDBOperations;
import mil.nga.giat.geowave.mapreduce.BaseMapReduceDataStore;

public class DynamoDBDataStore extends
		BaseMapReduceDataStore
{
	public final static String TYPE = "dynamodb";

	public DynamoDBDataStore(
			final DynamoDBOperations operations ) {
		this(
				new IndexStoreImpl(
						operations,
						operations.getOptions().getBaseOptions()),
				new AdapterStoreImpl(
						operations,
						operations.getOptions().getBaseOptions()),
				new DataStatisticsStoreImpl(
						operations,
						operations.getOptions().getBaseOptions()),
				new AdapterIndexMappingStoreImpl(
						operations,
						operations.getOptions().getBaseOptions()),
				new SecondaryIndexStoreImpl(),
				operations,
				operations.getOptions().getBaseOptions(),
				new InternalAdapterStoreImpl(
						operations));
	}

	public DynamoDBDataStore(
			final IndexStore indexStore,
			final PersistentAdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final DynamoDBOperations operations,
			final DataStoreOptions options,
			final InternalAdapterStore internalAdapterStore ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				operations,
				options,
				internalAdapterStore);

		secondaryIndexDataStore.setDataStore(this);
	}
}