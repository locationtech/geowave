package mil.nga.giat.geowave.analytic.store;

import java.util.Map;

import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;

public class PersistableIndexStore extends
		PersistableStore<IndexStore>
{
	public PersistableIndexStore() {
		super();
	}

	public PersistableIndexStore(
			final GenericStoreCommandLineOptions<IndexStore> cliOptions ) {
		super(
				cliOptions);
	}

	@Override
	protected GenericStoreCommandLineOptions<IndexStore> getCLIOptions(
			final Map<String, String> configOptions,
			final String namespace,
			final String factoryName ) {
		final IndexStoreFactorySpi indexStoreFactory = GeoWaveStoreFinder.getRegisteredIndexStoreFactories().get(
				factoryName);
		return new IndexStoreCommandLineOptions(
				indexStoreFactory,
				ConfigUtils.valuesFromStrings(
						configOptions,
						indexStoreFactory.getOptions()),
				namespace);
	}
}
