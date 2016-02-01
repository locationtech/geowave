package mil.nga.giat.geowave.analytic.param;

import java.util.Map;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.store.PersistableIndexStore;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexStoreParameterHelper implements
		ParameterHelper<PersistableIndexStore>
{
	final static Logger LOGGER = LoggerFactory.getLogger(IndexStoreParameterHelper.class);

	@Override
	public Class<PersistableIndexStore> getBaseClass() {
		return PersistableIndexStore.class;
	}

	@Override
	public Option[] getOptions() {
		final Options allOptions = new Options();
		IndexStoreCommandLineOptions.applyOptions(allOptions);
		return (Option[]) allOptions.getOptions().toArray(
				new Option[] {});
	}

	@Override
	public CommandLineResult<PersistableIndexStore> getValue(
			final Options allOptions,
			final CommandLine commandLine )
			throws ParseException {
		final CommandLineResult<IndexStoreCommandLineOptions> retVal = IndexStoreCommandLineOptions.parseOptions(
				allOptions,
				commandLine);
		return new CommandLineResult<PersistableIndexStore>(
				new PersistableIndexStore(
						retVal.getResult()),
				retVal.isCommandLineChange(),
				retVal.getCommandLine());
	}

	@Override
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final PersistableIndexStore value ) {
		final GenericStoreCommandLineOptions<IndexStore> options = value.getCliOptions();
		GeoWaveInputFormat.setIndexStoreName(
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
	public PersistableIndexStore getValue(
			final JobContext context,
			final Class<?> scope,
			final PersistableIndexStore defaultValue ) {
		final Map<String, String> configOptions = GeoWaveInputFormat.getStoreConfigOptions(context);
		final String indexStoreName = GeoWaveInputFormat.getIndexStoreName(context);
		final String geowaveNamespace = GeoWaveInputFormat.getGeoWaveNamespace(context);
		if ((indexStoreName != null) && (!indexStoreName.isEmpty())) {
			final IndexStoreFactorySpi factory = GeoWaveStoreFinder.getRegisteredIndexStoreFactories().get(
					indexStoreName);
			return new PersistableIndexStore(
					new IndexStoreCommandLineOptions(
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
	public PersistableIndexStore getValue(
			final PropertyManagement propertyManagement ) {
		try {
			return (PersistableIndexStore) propertyManagement.getProperty(StoreParameters.StoreParam.INDEX_STORE);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to deserialize index store",
					e);
			return null;
		}
	}

	@Override
	public void setValue(
			final PropertyManagement propertyManagement,
			final PersistableIndexStore value ) {
		propertyManagement.store(
				StoreParameters.StoreParam.INDEX_STORE,
				value);
	}
}
