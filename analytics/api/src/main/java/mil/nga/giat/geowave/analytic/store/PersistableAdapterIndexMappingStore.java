package mil.nga.giat.geowave.analytic.store;

import java.util.Map;

import mil.nga.giat.geowave.core.cli.AdapterIndexMappingStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStoreFactorySpi;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;

public class PersistableAdapterIndexMappingStore extends
		PersistableStore<AdapterIndexMappingStore>
{
	public PersistableAdapterIndexMappingStore() {
		super();
	}

	public PersistableAdapterIndexMappingStore(
			final GenericStoreCommandLineOptions<AdapterIndexMappingStore> cliOptions ) {
		super(
				cliOptions);
	}

	@Override
	protected GenericStoreCommandLineOptions<AdapterIndexMappingStore> getCLIOptions(
			final Map<String, String> configOptions,
			final String namespace,
			final String factoryName ) {
		final AdapterIndexMappingStoreFactorySpi adapterStoreFactory = GeoWaveStoreFinder.getRegisteredAdapterIndexMappingStoreFactories().get(
				factoryName);
		return new AdapterIndexMappingStoreCommandLineOptions(
				adapterStoreFactory,
				ConfigUtils.valuesFromStrings(
						configOptions,
						adapterStoreFactory.getOptions()),
				namespace);
	}
}
