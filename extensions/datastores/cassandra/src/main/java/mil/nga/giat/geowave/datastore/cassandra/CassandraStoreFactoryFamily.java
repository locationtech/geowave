package mil.nga.giat.geowave.datastore.cassandra;

import mil.nga.giat.geowave.core.store.BaseDataStoreFamily;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;

public class CassandraStoreFactoryFamily extends
		BaseDataStoreFamily
{
	private static final String TYPE = "cassandra";
	private static final String DESCRIPTION = "A GeoWave store backed by tables in Apache Cassandra";

	public CassandraStoreFactoryFamily() {
		super(
				TYPE,
				DESCRIPTION,
				new CassandraFactoryHelper());
	}

	@Override
	public GenericStoreFactory<DataStore> getDataStoreFactory() {
		return new CassandraDataStoreFactory(
				TYPE,
				DESCRIPTION,
				new CassandraFactoryHelper());
	}
}
