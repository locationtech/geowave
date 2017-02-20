package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.store.BaseDataStoreFamily;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStoreFactory;

public class AccumuloStoreFactoryFamily extends
		BaseDataStoreFamily
{
	public final static String TYPE = "accumulo";
	private static final String DESCRIPTION = "A GeoWave store backed by tables in Apache Accumulo";

	public AccumuloStoreFactoryFamily() {
		super(
				TYPE,
				DESCRIPTION,
				new AccumuloFactoryHelper());
	}

	@Override
	public GenericStoreFactory<DataStore> getDataStoreFactory() {
		return new AccumuloDataStoreFactory(
				TYPE,
				DESCRIPTION,
				new AccumuloFactoryHelper());
	}

	@Override
	public GenericStoreFactory<SecondaryIndexDataStore> getSecondaryIndexDataStore() {
		return new AccumuloSecondaryIndexDataStoreFactory(
				TYPE,
				DESCRIPTION,
				new AccumuloFactoryHelper());
	}
}
