package mil.nga.giat.geowave.core.cli;

import java.util.Map;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStoreFactorySpi;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class AdapterIndexMappingStoreCommandLineOptions extends
		GenericStoreCommandLineOptions<AdapterIndexMappingStore>
{
	public static final String ADAPTER_INDEX_STORE_NAME_KEY = "adapterindexstore";

	public AdapterIndexMappingStoreCommandLineOptions(
			final GenericStoreFactory<AdapterIndexMappingStore> factory,
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
				new AdapterIndexMappingStoreCommandLineHelper());
	}

	public static CommandLineResult<AdapterIndexMappingStoreCommandLineOptions> parseOptions(
			final Options options,
			final CommandLine commandLine )
			throws ParseException {
		return parseOptions(
				null,
				options,
				commandLine);
	}

	public static CommandLineResult<AdapterIndexMappingStoreCommandLineOptions> parseOptions(
			final String prefix,
			final Options options,
			final CommandLine commandLine )
			throws ParseException {
		return (CommandLineResult) parseOptions(
				prefix,
				options,
				commandLine,
				new AdapterIndexMappingStoreCommandLineHelper());
	}

	public static CommandLineResult<AdapterIndexMappingStoreCommandLineOptions> parseOptions(
			final Options options,
			final CommandLineOptions commandLine )
			throws ParseException {
		return parseOptions(
				null,
				options,
				commandLine);
	}

	public static CommandLineResult<AdapterIndexMappingStoreCommandLineOptions> parseOptions(
			final String prefix,
			final Options options,
			final CommandLineOptions commandLine )
			throws ParseException {
		return (CommandLineResult) parseOptions(
				prefix,
				options,
				commandLine,
				new AdapterIndexMappingStoreCommandLineHelper());
	}

	public static AdapterIndexMappingStoreFactorySpi getSelectedStore(
			final CommandLineOptions commandLine )
			throws ParseException {
		final AdapterIndexMappingStoreCommandLineHelper helper = new AdapterIndexMappingStoreCommandLineHelper();
		return getSelectedStore(
				helper.getOptionName(),
				commandLine,
				helper);
	}

	private static class AdapterIndexMappingStoreCommandLineHelper implements
			CommandLineHelper<AdapterIndexMappingStore, AdapterIndexMappingStoreFactorySpi>
	{
		@Override
		public Map<String, AdapterIndexMappingStoreFactorySpi> getRegisteredFactories() {
			return GeoWaveStoreFinder.getRegisteredAdapterIndexMappingStoreFactories();
		}

		@Override
		public String getOptionName() {
			return ADAPTER_INDEX_STORE_NAME_KEY;
		}

		@Override
		public GenericStoreCommandLineOptions<AdapterIndexMappingStore> createCommandLineOptions(
				final GenericStoreFactory<AdapterIndexMappingStore> factory,
				final Map<String, Object> configOptions,
				final String namespace ) {
			return new AdapterIndexMappingStoreCommandLineOptions(
					factory,
					configOptions,
					namespace);
		}
	}
}
