package mil.nga.giat.geowave.analytic.param;

import java.util.Map;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.store.PersistableAdapterStore;
import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;
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

public class AdapterStoreParameterHelper implements
		ParameterHelper<PersistableAdapterStore>
{
	final static Logger LOGGER = LoggerFactory.getLogger(AdapterStoreParameterHelper.class);

	@Override
	public Class<PersistableAdapterStore> getBaseClass() {
		return PersistableAdapterStore.class;
	}

	@Override
	public Option[] getOptions() {
		final Options allOptions = new Options();
		AdapterStoreCommandLineOptions.applyOptions(allOptions);
		return (Option[]) allOptions.getOptions().toArray(
				new Option[] {});
	}

	@Override
	public CommandLineResult<PersistableAdapterStore> getValue(
			final Options allOptions,
			final CommandLine commandLine )
			throws ParseException {
		final CommandLineResult<AdapterStoreCommandLineOptions> retVal = AdapterStoreCommandLineOptions.parseOptions(
				allOptions,
				commandLine);
		return new CommandLineResult<PersistableAdapterStore>(
				new PersistableAdapterStore(
						retVal.getResult()),
				retVal.isCommandLineChange(),
				retVal.getCommandLine());
	}

	@Override
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final PersistableAdapterStore value ) {
		final GenericStoreCommandLineOptions<AdapterStore> options = value.getCliOptions();
		GeoWaveInputFormat.setAdapterStoreName(
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
	public PersistableAdapterStore getValue(
			final JobContext context,
			final Class<?> scope,
			final PersistableAdapterStore defaultValue ) {
		final Map<String, String> configOptions = GeoWaveInputFormat.getStoreConfigOptions(context);
		final String adapterStoreName = GeoWaveInputFormat.getAdapterStoreName(context);
		final String geowaveNamespace = GeoWaveInputFormat.getGeoWaveNamespace(context);
		if ((adapterStoreName != null) && (!adapterStoreName.isEmpty())) {
			final AdapterStoreFactorySpi factory = GeoWaveStoreFinder.getRegisteredAdapterStoreFactories().get(
					adapterStoreName);
			return new PersistableAdapterStore(
					new AdapterStoreCommandLineOptions(
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
	public PersistableAdapterStore getValue(
			final PropertyManagement propertyManagement ) {
		try {
			return (PersistableAdapterStore) propertyManagement.getProperty(StoreParameters.StoreParam.ADAPTER_STORE);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to deserialize adapter store",
					e);
			return null;
		}
	}

	@Override
	public void setValue(
			final PropertyManagement propertyManagement,
			final PersistableAdapterStore value ) {
		propertyManagement.store(
				StoreParameters.StoreParam.ADAPTER_STORE,
				value);
	}
}
