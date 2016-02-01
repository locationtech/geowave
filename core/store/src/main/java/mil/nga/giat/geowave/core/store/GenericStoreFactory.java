package mil.nga.giat.geowave.core.store;

import java.util.Map;

import mil.nga.giat.geowave.core.store.config.AbstractConfigOption;

public interface GenericStoreFactory<T> extends
		GenericFactory
{
	public T createStore(
			Map<String, Object> configOptions,
			String namespace );

	public AbstractConfigOption<?>[] getOptions();
}
