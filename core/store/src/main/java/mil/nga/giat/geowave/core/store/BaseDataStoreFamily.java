package mil.nga.giat.geowave.core.store;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreFactory;
import mil.nga.giat.geowave.core.store.metadata.InternalAdapterStoreFactory;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreFactory;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreFactory;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreFactory;
import mil.nga.giat.geowave.core.store.metadata.SecondaryIndexStoreFactory;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperationsFactory;

public class BaseDataStoreFamily implements
		StoreFactoryFamilySpi
{
	private final String typeName;
	private final String description;
	private final StoreFactoryHelper helper;

	public BaseDataStoreFamily(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super();
		this.typeName = typeName;
		this.description = description;
		this.helper = helper;
	}

	@Override
	public String getType() {
		return typeName;
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public GenericStoreFactory<DataStore> getDataStoreFactory() {
		return new DataStoreFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<DataStatisticsStore> getDataStatisticsStoreFactory() {
		return new DataStatisticsStoreFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<IndexStore> getIndexStoreFactory() {
		return new IndexStoreFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<PersistentAdapterStore> getAdapterStoreFactory() {
		return new AdapterStoreFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<AdapterIndexMappingStore> getAdapterIndexMappingStoreFactory() {
		return new AdapterIndexMappingStoreFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<SecondaryIndexDataStore> getSecondaryIndexDataStore() {
		return new SecondaryIndexStoreFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<DataStoreOperations> getDataStoreOperationsFactory() {
		return new DataStoreOperationsFactory(
				typeName,
				description,
				helper);
	}

	@Override
	public GenericStoreFactory<InternalAdapterStore> getInternalAdapterStoreFactory() {
		return new InternalAdapterStoreFactory(
				typeName,
				description,
				helper);
	}

}
