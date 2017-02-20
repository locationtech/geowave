package mil.nga.giat.geowave.core.store.memory;

import mil.nga.giat.geowave.core.store.BaseDataStoreFamily;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class MemoryStoreFactoryFamily extends
		BaseDataStoreFamily implements
		StoreFactoryFamilySpi
{
	private static final String TYPE = "memory";
	private static final String DESCRIPTION = "A GeoWave store that is in memory typically only used for test purposes";

	public MemoryStoreFactoryFamily() {
		super(
				TYPE,
				DESCRIPTION,
				new MemoryFactoryHelper());
	}
}
