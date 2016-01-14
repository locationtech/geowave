package mil.nga.giat.geowave.datastore.accumulo;

import mil.nga.giat.geowave.core.store.GenericFactory;

abstract public class AbstractAccumuloFactory implements
		GenericFactory
{
	private static final String NAME = "accumulo";
	private static final String DESCRIPTION = "A GeoWave store backed by tables in Apache Accumulo";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}
}
