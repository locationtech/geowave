package mil.nga.giat.geowave.core.store.memory;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.config.ConfigOption;

abstract public class AbstractMemoryStoreFactory<T> extends
		AbstractMemoryFactory implements
		GenericStoreFactory<T>
{
}
