package mil.nga.giat.geowave.core.store.memory;

import mil.nga.giat.geowave.core.store.GenericFactory;

abstract public class AbstractMemoryFactory implements
		GenericFactory
{
	@Override
	public String getName() {
		return "memory";
	}

	@Override
	public String getDescription() {
		return "A GeoWave store that is in memory typically only used for test purposes";
	}
}
