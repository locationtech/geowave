package mil.nga.giat.geowave.analytic.param;

import java.util.Map;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.store.PersistableDataStore;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStoreParameterHelper implements
		ParameterHelper<PersistableDataStore>
{
	final static Logger LOGGER = LoggerFactory.getLogger(DataStoreParameterHelper.class);

	@Override
	public Class<PersistableDataStore> getBaseClass() {
		return PersistableDataStore.class;
	}

	@Override
	public Option[] getOptions() {
		final Options allOptions = new Options();
		DataStoreCommandLineOptions.applyOptions(allOptions);
		return (Option[]) allOptions.getOptions().toArray(
				new Option[] {});
	}

	@Override
	public CommandLineResult<PersistableDataStore> getValue(
			final Options allOptions,
			final CommandLine commandLine )
			throws ParseException {
		final CommandLineResult<DataStoreCommandLineOptions> retVal = DataStoreCommandLineOptions.parseOptions(
				allOptions,
				commandLine);
		return new CommandLineResult<PersistableDataStore>(
				new PersistableDataStore(
						retVal.getResult()),
				retVal.isCommandLineChange(),
				retVal.getCommandLine());
	}

	@Override
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final PersistableDataStore value ) {
		final GenericStoreCommandLineOptions<DataStore> options = value.getCliOptions();
		GeoWaveInputFormat.setDataStoreName(
				config,
				options.getFactory().getName());
		GeoWaveInputFormat.setStoreConfigOptions(
				config,
				ConfigUtils.valuesToStrings(
						options.getConfigOptions(),
						options.getFactory().getOptions()));
		GeoWaveInputFormat.setGeoWaveNamespace(
				config,
				options.getNamespace());
	}

	@Override
	public PersistableDataStore getValue(
			final JobContext context,
			final Class<?> scope,
			final PersistableDataStore defaultValue ) {
		final Map<String, String> configOptions = GeoWaveInputFormat.getStoreConfigOptions(context);
		final String dataStoreName = GeoWaveInputFormat.getDataStoreName(context);
		final String geowaveNamespace = GeoWaveInputFormat.getGeoWaveNamespace(context);
		if ((dataStoreName != null) && (!dataStoreName.isEmpty())) {
			final DataStoreFactorySpi factory = GeoWaveStoreFinder.getRegisteredDataStoreFactories().get(
					dataStoreName);
			return new PersistableDataStore(
					new DataStoreCommandLineOptions(
							factory,
							ConfigUtils.valuesFromStrings(
									configOptions,
									factory.getOptions()),
							geowaveNamespace));
		}
		else {
			return defaultValue;
		}

	}

	@Override
	public PersistableDataStore getValue(
			final PropertyManagement propertyManagement ) {
		try {
			return (PersistableDataStore) propertyManagement.getProperty(StoreParameters.StoreParam.DATA_STORE);
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
			final PersistableDataStore value ) {
		propertyManagement.store(
				StoreParameters.StoreParam.DATA_STORE,
				value);
	}
}
