package mil.nga.giat.geowave.core.cli;

import java.util.Map;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class IndexStoreCommandLineOptions extends
		GenericStoreCommandLineOptions<IndexStore>
{
	public static final String INDEX_STORE_NAME_KEY = "indexstore";

	public IndexStoreCommandLineOptions(
			final GenericStoreFactory<IndexStore> factory,
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
				new IndexStoreCommandLineHelper());
	}

	public static CommandLineResult<IndexStoreCommandLineOptions> parseOptions(
			final Options options,
			final CommandLine commandLine )
			throws ParseException {
		return parseOptions(
				null,
				options,
				commandLine);
	}

	public static CommandLineResult<IndexStoreCommandLineOptions> parseOptions(
			final String prefix,
			final Options options,
			final CommandLine commandLine )
			throws ParseException {
		return (CommandLineResult) parseOptions(
				prefix,
				options,
				commandLine,
				new IndexStoreCommandLineHelper());
	}

	public static CommandLineResult<IndexStoreCommandLineOptions> parseOptions(
			final Options options,
			final CommandLineOptions commandLine )
			throws ParseException {
		return parseOptions(
				null,
				options,
				commandLine);
	}

	public static CommandLineResult<IndexStoreCommandLineOptions> parseOptions(
			final String prefix,
			final Options options,
			final CommandLineOptions commandLine )
			throws ParseException {
		return (CommandLineResult) parseOptions(
				prefix,
				options,
				commandLine,
				new IndexStoreCommandLineHelper());
	}

	public static IndexStoreFactorySpi getSelectedStore(
			final CommandLineOptions commandLine )
			throws ParseException {
		final IndexStoreCommandLineHelper helper = new IndexStoreCommandLineHelper();
		return getSelectedStore(
				helper.getOptionName(),
				commandLine,
				helper);
	}

	private static class IndexStoreCommandLineHelper implements
			CommandLineHelper<IndexStore, IndexStoreFactorySpi>
	{
		@Override
		public Map<String, IndexStoreFactorySpi> getRegisteredFactories() {
			return GeoWaveStoreFinder.getRegisteredIndexStoreFactories();
		}

		@Override
		public String getOptionName() {
			return INDEX_STORE_NAME_KEY;
		}

		@Override
		public GenericStoreCommandLineOptions<IndexStore> createCommandLineOptions(
				final GenericStoreFactory<IndexStore> factory,
				final Map<String, Object> configOptions,
				final String namespace ) {
			return new IndexStoreCommandLineOptions(
					factory,
					configOptions,
					namespace);
		}
	}
}
