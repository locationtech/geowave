package mil.nga.giat.geowave.datastore.cassandra;

import mil.nga.giat.geowave.core.store.GenericFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;

abstract public class AbstractCassandraFactory implements
		GenericFactory
{
	private static final String TYPE = "cassandra";
	private static final String DESCRIPTION = "A GeoWave store backed by tables in Apache Cassandra";

	@Override
	public String getType() {
		return TYPE;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

	/**
	 * This helps implementation of child classes by returning the default
	 * Cassandra options that are required.
	 *
	 * @return
	 */
	public StoreFactoryOptions createOptionsInstance() {
		return new CassandraRequiredOptions();
	}
}
