package mil.nga.giat.geowave.core.cli;

import java.util.Map;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class DataStatisticsStoreCommandLineOptions extends
		GenericStoreCommandLineOptions<DataStatisticsStore>
{
	public static final String DATA_STATISTICS_STORE_NAME_KEY = "statstore";

	public DataStatisticsStoreCommandLineOptions(
			final GenericStoreFactory<DataStatisticsStore> factory,
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
				new DataStatisticsStoreCommandLineHelper());
	}

	public static CommandLineResult<DataStatisticsStoreCommandLineOptions> parseOptions(
			final Options options,
			final CommandLine commandLine )
			throws ParseException {
		return parseOptions(
				null,
				options,
				commandLine);
	}

	public static CommandLineResult<DataStatisticsStoreCommandLineOptions> parseOptions(
			final String prefix,
			final Options options,
			final CommandLine commandLine )
			throws ParseException {
		return (CommandLineResult) parseOptions(
				prefix,
				options,
				commandLine,
				new DataStatisticsStoreCommandLineHelper());
	}

	public static CommandLineResult<DataStatisticsStoreCommandLineOptions> parseOptions(
			final Options options,
			final CommandLineOptions commandLine )
			throws ParseException {
		return parseOptions(
				null,
				options,
				commandLine);
	}

	public static CommandLineResult<DataStatisticsStoreCommandLineOptions> parseOptions(
			final String prefix,
			final Options options,
			final CommandLineOptions commandLine )
			throws ParseException {
		return (CommandLineResult) parseOptions(
				prefix,
				options,
				commandLine,
				new DataStatisticsStoreCommandLineHelper());
	}

	public static DataStatisticsStoreFactorySpi getSelectedStore(
			final CommandLineOptions commandLine )
			throws ParseException {
		final DataStatisticsStoreCommandLineHelper helper = new DataStatisticsStoreCommandLineHelper();
		return getSelectedStore(
				helper.getOptionName(),
				commandLine,
				helper);
	}

	private static class DataStatisticsStoreCommandLineHelper implements
			CommandLineHelper<DataStatisticsStore, DataStatisticsStoreFactorySpi>
	{
		@Override
		public Map<String, DataStatisticsStoreFactorySpi> getRegisteredFactories() {
			return GeoWaveStoreFinder.getRegisteredDataStatisticsStoreFactories();
		}

		@Override
		public String getOptionName() {
			return DATA_STATISTICS_STORE_NAME_KEY;
		}

		@Override
		public GenericStoreCommandLineOptions<DataStatisticsStore> createCommandLineOptions(
				final GenericStoreFactory<DataStatisticsStore> factory,
				final Map<String, Object> configOptions,
				final String namespace ) {
			return new DataStatisticsStoreCommandLineOptions(
					factory,
					configOptions,
					namespace);
		}
	}
}