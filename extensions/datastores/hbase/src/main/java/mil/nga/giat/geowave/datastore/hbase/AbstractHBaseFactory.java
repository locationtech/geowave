package mil.nga.giat.geowave.datastore.hbase;

import mil.nga.giat.geowave.core.store.GenericFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseRequiredOptions;

abstract public class AbstractHBaseFactory implements
		GenericFactory
{
	private static final String TYPE = HBaseDataStore.TYPE;
	private static final String DESCRIPTION = "A GeoWave store backed by tables in Apache HBase";

	@Override
	public String getType() {
		return TYPE;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

	/**
	 * This helps implementation of child classes by returning the default HBase
	 * options that are required.
	 * 
	 * @return
	 */
	public StoreFactoryOptions createOptionsInstance() {
		return new HBaseRequiredOptions();
	}
}
