package mil.nga.giat.geowave.datastore.accumulo;

import java.util.Map;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.config.AbstractConfigOption;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractAccumuloStoreFactory<T> extends
		AbstractAccumuloFactory implements
		GenericStoreFactory<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractAccumuloStoreFactory.class);

	@Override
	public AbstractConfigOption<?>[] getOptions() {
		return BasicAccumuloOperations.getOptions();
	}

	protected BasicAccumuloOperations createOperations(
			final Map<String, Object> configOptions,
			final String namespace ) {
		try {
			return BasicAccumuloOperations.createOperations(
					configOptions,
					namespace);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to create Accumulo operations from config options",
					e);
			return null;
		}
	}

}
