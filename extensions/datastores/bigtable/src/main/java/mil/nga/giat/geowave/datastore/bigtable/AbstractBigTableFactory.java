package mil.nga.giat.geowave.datastore.bigtable;

import mil.nga.giat.geowave.core.store.GenericFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.bigtable.operations.config.BigTableOptions;

abstract public class AbstractBigTableFactory implements
		GenericFactory
{
	private static final String TYPE = "bigtable";
	private static final String DESCRIPTION = "A GeoWave store backed by tables in Google's Cloud BigTable";

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
	 * BigTable options that are required.
	 *
	 * @return
	 */
	public StoreFactoryOptions createOptionsInstance() {
		return new BigTableOptions();
	}
}
