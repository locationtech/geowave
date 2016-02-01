package mil.nga.giat.geowave.core.store.memory;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.config.AbstractConfigOption;

abstract public class AbstractMemoryStoreFactory<T> extends
		AbstractMemoryFactory implements
		GenericStoreFactory<T>
{
	@Override
	public AbstractConfigOption<?>[] getOptions() {
		return new AbstractConfigOption<?>[] {};
	}
}
