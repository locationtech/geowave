package mil.nga.giat.geowave.core.cli;

import java.util.Map;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class DataStoreCommandLineOptions extends
		GenericStoreCommandLineOptions<DataStore>
{
	public static final String DATA_STORE_NAME_KEY = "datastore";

	public DataStoreCommandLineOptions(
			final GenericStoreFactory<DataStore> factory,
			final Map<String, Object> configOptions,
			final String namespace ) {
		super(
				factory,
				configOptions,
				namespace);
	}

	public static void applyOptions(
			final Options allOptions ) {
		applyOptions(
				null,
				allOptions);
	}

	public static void applyOptions(
			final String prefix,
			final Options allOptions ) {
		applyOptions(
				prefix,
				allOptions,
				new DataStoreCommandLineHelper());
	}

	public static CommandLineResult<DataStoreCommandLineOptions> parseOptions(
			final Options options,
			final CommandLine commandLine )
			throws ParseException {
		return parseOptions(
				null,
				options,
				commandLine);
	}

	public static CommandLineResult<DataStoreCommandLineOptions> parseOptions(
			final String prefix,
			final Options options,
			final CommandLine commandLine )
			throws ParseException {
		return (CommandLineResult) parseOptions(
				prefix,
				options,
				commandLine,
				new DataStoreCommandLineHelper());
	}

	public static CommandLineResult<DataStoreCommandLineOptions> parseOptions(
			final Options options,
			final CommandLineOptions commandLine )
			throws ParseException {
		return parseOptions(
				null,
				options,
				commandLine);
	}

	public static CommandLineResult<DataStoreCommandLineOptions> parseOptions(
			final String prefix,
			final Options options,
			final CommandLineOptions commandLine )
			throws ParseException {
		return (CommandLineResult) parseOptions(
				prefix,
				options,
				commandLine,
				new DataStoreCommandLineHelper());
	}

	public static DataStoreFactorySpi getSelectedStore(
			final CommandLineOptions commandLine )
			throws ParseException {
		final DataStoreCommandLineHelper helper = new DataStoreCommandLineHelper();
		return getSelectedStore(
				helper.getOptionName(),
				commandLine,
				helper);
	}

	private static class DataStoreCommandLineHelper implements
			CommandLineHelper<DataStore, DataStoreFactorySpi>
	{
		@Override
		public Map<String, DataStoreFactorySpi> getRegisteredFactories() {
			return GeoWaveStoreFinder.getRegisteredDataStoreFactories();
		}

		@Override
		public String getOptionName() {
			return DATA_STORE_NAME_KEY;
		}

		@Override
		public GenericStoreCommandLineOptions<DataStore> createCommandLineOptions(
				final GenericStoreFactory<DataStore> factory,
				final Map<String, Object> configOptions,
				final String namespace ) {
			return new DataStoreCommandLineOptions(
					factory,
					configOptions,
					namespace);
		}
	}
}
