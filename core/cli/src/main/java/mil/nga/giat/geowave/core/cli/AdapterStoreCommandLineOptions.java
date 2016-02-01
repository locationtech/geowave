package mil.nga.giat.geowave.core.cli;

import java.util.Map;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class AdapterStoreCommandLineOptions extends
		GenericStoreCommandLineOptions<AdapterStore>
{
	public static final String ADAPTER_STORE_NAME_KEY = "adapterstore";

	public AdapterStoreCommandLineOptions(
			final GenericStoreFactory<AdapterStore> factory,
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
				new AdapterStoreCommandLineHelper());
	}

	public static CommandLineResult<AdapterStoreCommandLineOptions> parseOptions(
			final Options options,
			final CommandLine commandLine )
			throws ParseException {
		return parseOptions(
				null,
				options,
				commandLine);
	}

	public static CommandLineResult<AdapterStoreCommandLineOptions> parseOptions(
			final String prefix,
			final Options options,
			final CommandLine commandLine )
			throws ParseException {
		return (CommandLineResult) parseOptions(
				prefix,
				options,
				commandLine,
				new AdapterStoreCommandLineHelper());
	}

	public static CommandLineResult<AdapterStoreCommandLineOptions> parseOptions(
			final Options options,
			final CommandLineOptions commandLine )
			throws ParseException {
		return parseOptions(
				null,
				options,
				commandLine);
	}

	public static CommandLineResult<AdapterStoreCommandLineOptions> parseOptions(
			final String prefix,
			final Options options,
			final CommandLineOptions commandLine )
			throws ParseException {
		return (CommandLineResult) parseOptions(
				prefix,
				options,
				commandLine,
				new AdapterStoreCommandLineHelper());
	}

	public static AdapterStoreFactorySpi getSelectedStore(
			final CommandLineOptions commandLine )
			throws ParseException {
		final AdapterStoreCommandLineHelper helper = new AdapterStoreCommandLineHelper();
		return getSelectedStore(
				helper.getOptionName(),
				commandLine,
				helper);
	}

	private static class AdapterStoreCommandLineHelper implements
			CommandLineHelper<AdapterStore, AdapterStoreFactorySpi>
	{
		@Override
		public Map<String, AdapterStoreFactorySpi> getRegisteredFactories() {
			return GeoWaveStoreFinder.getRegisteredAdapterStoreFactories();
		}

		@Override
		public String getOptionName() {
			return ADAPTER_STORE_NAME_KEY;
		}

		@Override
		public GenericStoreCommandLineOptions<AdapterStore> createCommandLineOptions(
				final GenericStoreFactory<AdapterStore> factory,
				final Map<String, Object> configOptions,
				final String namespace ) {
			return new AdapterStoreCommandLineOptions(
					factory,
					configOptions,
					namespace);
		}
	}
}
