package mil.nga.giat.geowave.datastore.hbase;

import mil.nga.giat.geowave.core.store.GenericFactory;

abstract public class AbstractHBaseFactory implements
		GenericFactory
{
	private static final String NAME = "hbase";
	private static final String DESCRIPTION = "A GeoWave store backed by tables in Apache HBase";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}
}
