package mil.nga.giat.geowave.analytic.mapreduce;

import java.util.Arrays;
import java.util.Collection;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.StoreParameters.StoreParam;
import mil.nga.giat.geowave.analytic.store.PersistableDataStore;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;

import org.apache.hadoop.conf.Configuration;

public class GeoWaveOutputFormatConfiguration implements
		FormatConfiguration
{
	/**
	 * Captures the state, but the output format is flexible enough to deal with
	 * both.
	 * 
	 */
	protected boolean isDataWritable = false;

	@Override
	public void setup(
			final PropertyManagement runTimeProperties,
			final Configuration configuration )
			throws Exception {
		final GenericStoreCommandLineOptions<DataStore> dataStoreOptions = ((PersistableDataStore) runTimeProperties.getProperty(StoreParam.DATA_STORE)).getCliOptions();
		GeoWaveOutputFormat.setDataStoreName(
				configuration,
				dataStoreOptions.getFactory().getName());
		GeoWaveOutputFormat.setStoreConfigOptions(
				configuration,
				ConfigUtils.valuesToStrings(
						dataStoreOptions.getConfigOptions(),
						dataStoreOptions.getFactory().getOptions()));
		GeoWaveOutputFormat.setGeoWaveNamespace(
				configuration,
				dataStoreOptions.getNamespace());

	}

	@Override
	public Class<?> getFormatClass() {
		return GeoWaveOutputFormat.class;
	}

	@Override
	public boolean isDataWritable() {
		return isDataWritable;
	}

	@Override
	public void setDataIsWritable(
			final boolean isWritable ) {
		isDataWritable = isWritable;
	}

	@Override
	public Collection<ParameterEnum<?>> getParameters() {
		return Arrays.asList(new ParameterEnum<?>[] {
			StoreParam.DATA_STORE
		});
	}
}
