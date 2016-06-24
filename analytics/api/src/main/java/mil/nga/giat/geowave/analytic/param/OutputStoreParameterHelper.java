package mil.nga.giat.geowave.analytic.param;

import java.util.Map;

import org.apache.directory.api.util.exception.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.store.PersistableStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;

public class OutputStoreParameterHelper implements
		ParameterHelper<PersistableStore>
{
	final static Logger LOGGER = LoggerFactory.getLogger(OutputStoreParameterHelper.class);

	@Override
	public Class<PersistableStore> getBaseClass() {
		return PersistableStore.class;
	}

	@Override
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final PersistableStore value ) {
		final DataStorePluginOptions options = value.getDataStoreOptions();
		GeoWaveOutputFormat.setDataStoreName(
				config,
				options.getType());
		GeoWaveOutputFormat.setStoreConfigOptions(
				config,
				options.getFactoryOptionsAsMap());

	}

	@Override
	public PersistableStore getValue(
			final JobContext context,
			final Class<?> scope,
			final PersistableStore defaultValue ) {
		final Map<String, String> configOptions = GeoWaveOutputFormat.getStoreConfigOptions(context);
		final String dataStoreName = GeoWaveOutputFormat.getDataStoreName(context);
		if ((dataStoreName != null) && (!dataStoreName.isEmpty())) {
			// Load the plugin, and populate the options object.
			DataStorePluginOptions pluginOptions = new DataStorePluginOptions(
					dataStoreName,
					configOptions);
			return new PersistableStore(
					pluginOptions);
		}
		else {
			return defaultValue;
		}
	}

	@Override
	public PersistableStore getValue(
			final PropertyManagement propertyManagement ) {
		try {
			return (PersistableStore) propertyManagement.getProperty(StoreParameters.StoreParam.OUTPUT_STORE);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to deserialize data store",
					e);
			return null;
		}
	}

	@Override
	public void setValue(
			final PropertyManagement propertyManagement,
			final PersistableStore value ) {
		propertyManagement.store(
				StoreParameters.StoreParam.OUTPUT_STORE,
				value);
	}
}
