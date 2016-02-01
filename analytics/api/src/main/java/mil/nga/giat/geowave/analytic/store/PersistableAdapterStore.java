package mil.nga.giat.geowave.analytic.store;

import java.util.Map;

import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;

public class PersistableAdapterStore extends
		PersistableStore<AdapterStore>
{
	public PersistableAdapterStore() {
		super();
	}

	public PersistableAdapterStore(
			final GenericStoreCommandLineOptions<AdapterStore> cliOptions ) {
		super(
				cliOptions);
	}

	@Override
	protected GenericStoreCommandLineOptions<AdapterStore> getCLIOptions(
			final Map<String, String> configOptions,
			final String namespace,
			final String factoryName ) {
		final AdapterStoreFactorySpi adapterStoreFactory = GeoWaveStoreFinder.getRegisteredAdapterStoreFactories().get(
				factoryName);
		return new AdapterStoreCommandLineOptions(
				adapterStoreFactory,
				ConfigUtils.valuesFromStrings(
						configOptions,
						adapterStoreFactory.getOptions()),
				namespace);
	}
}
