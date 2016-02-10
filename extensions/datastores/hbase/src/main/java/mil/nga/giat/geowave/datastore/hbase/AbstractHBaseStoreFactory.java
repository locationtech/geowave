package mil.nga.giat.geowave.datastore.hbase;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.config.AbstractConfigOption;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

abstract public class AbstractHBaseStoreFactory<T> extends
		AbstractHBaseFactory implements
		GenericStoreFactory<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractHBaseStoreFactory.class);

	@Override
	public AbstractConfigOption<?>[] getOptions() {
		return BasicHBaseOperations.getOptions();
	}

	protected BasicHBaseOperations createOperations(
			final Map<String, Object> configOptions,
			final String namespace ) {
		try {
			return BasicHBaseOperations.createOperations(
					configOptions,
					namespace);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to create HBase operations from config options",
					e);
			return null;
		}
	}

}
