package mil.nga.giat.geowave.analytic.param;

import java.util.Map;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.store.PersistableDataStatisticsStore;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStatisticsStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;
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

public class DataStatisticsStoreParameterHelper implements
		ParameterHelper<PersistableDataStatisticsStore>
{
	final static Logger LOGGER = LoggerFactory.getLogger(DataStatisticsStoreParameterHelper.class);

	@Override
	public Class<PersistableDataStatisticsStore> getBaseClass() {
		return PersistableDataStatisticsStore.class;
	}

	@Override
	public Option[] getOptions() {
		final Options allOptions = new Options();
		DataStatisticsStoreCommandLineOptions.applyOptions(allOptions);
		return (Option[]) allOptions.getOptions().toArray(
				new Option[] {});
	}

	@Override
	public CommandLineResult<PersistableDataStatisticsStore> getValue(
			final Options allOptions,
			final CommandLine commandLine )
			throws ParseException {
		final CommandLineResult<DataStatisticsStoreCommandLineOptions> retVal = DataStatisticsStoreCommandLineOptions.parseOptions(
				allOptions,
				commandLine);
		return new CommandLineResult<PersistableDataStatisticsStore>(
				new PersistableDataStatisticsStore(
						retVal.getResult()),
				retVal.isCommandLineChange(),
				retVal.getCommandLine());
	}

	@Override
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final PersistableDataStatisticsStore value ) {
		final GenericStoreCommandLineOptions<DataStatisticsStore> options = value.getCliOptions();
		GeoWaveInputFormat.setDataStatisticsStoreName(
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
	public PersistableDataStatisticsStore getValue(
			final JobContext context,
			final Class<?> scope,
			final PersistableDataStatisticsStore defaultValue ) {
		final Map<String, String> configOptions = GeoWaveInputFormat.getStoreConfigOptions(context);
		final String dataStatisticsStoreName = GeoWaveInputFormat.getDataStatisticsStoreName(context);
		final String geowaveNamespace = GeoWaveInputFormat.getGeoWaveNamespace(context);
		if ((dataStatisticsStoreName != null) && (!dataStatisticsStoreName.isEmpty())) {
			final DataStatisticsStoreFactorySpi factory = GeoWaveStoreFinder.getRegisteredDataStatisticsStoreFactories().get(
					dataStatisticsStoreName);
			return new PersistableDataStatisticsStore(
					new DataStatisticsStoreCommandLineOptions(
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
	public PersistableDataStatisticsStore getValue(
			final PropertyManagement propertyManagement ) {
		try {
			return (PersistableDataStatisticsStore) propertyManagement.getProperty(StoreParameters.StoreParam.DATA_STATISTICS_STORE);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to deserialize data statistics store",
					e);
			return null;
		}
	}

	@Override
	public void setValue(
			final PropertyManagement propertyManagement,
			final PersistableDataStatisticsStore value ) {
		propertyManagement.store(
				StoreParameters.StoreParam.DATA_STATISTICS_STORE,
				value);
	}
}
