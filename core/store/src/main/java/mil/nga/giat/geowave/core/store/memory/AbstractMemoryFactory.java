package mil.nga.giat.geowave.core.store.memory;

import mil.nga.giat.geowave.core.store.GenericFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

abstract public class AbstractMemoryFactory implements
		GenericFactory
{
	@Override
	public String getType() {
		return "memory";
	}

	@Override
	public String getDescription() {
		return "A GeoWave store that is in memory typically only used for test purposes";
	}

	/**
	 * Return the default options instance. This is actually a method that
	 * should be implemented by the individual factories, but is placed here
	 * since it's the same.
	 * 
	 * @return
	 */
	public StoreFactoryOptions createOptionsInstance() {
		return new MemoryRequiredOptions();
	}
}
