package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.store.GenericFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;

abstract public class AbstractAccumuloFactory implements
		GenericFactory
{
	private static final String NAME = AccumuloDataStore.TYPE;
	private static final String DESCRIPTION = "A GeoWave store backed by tables in Apache Accumulo";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

	/**
	 * This helps implementation of child classes by returning the default
	 * Accumulo options that are required.
	 * 
	 * @return
	 */
	public StoreFactoryOptions createOptionsInstance() {
		return new AccumuloRequiredOptions();
	}
}
