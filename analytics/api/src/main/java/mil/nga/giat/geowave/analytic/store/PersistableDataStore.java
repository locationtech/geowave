package mil.nga.giat.geowave.analytic.store;

import java.util.Map;

import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;

public class PersistableDataStore extends
		PersistableStore<DataStore>
{
	public PersistableDataStore() {
		super();
	}

	public PersistableDataStore(
			final GenericStoreCommandLineOptions<DataStore> cliOptions ) {
		super(
				cliOptions);
	}

	@Override
	protected GenericStoreCommandLineOptions<DataStore> getCLIOptions(
			final Map<String, String> configOptions,
			final String namespace,
			final String factoryName ) {
		final DataStoreFactorySpi dataStoreFactory = GeoWaveStoreFinder.getRegisteredDataStoreFactories().get(
				factoryName);
		return new DataStoreCommandLineOptions(
				dataStoreFactory,
				ConfigUtils.valuesFromStrings(
						configOptions,
						dataStoreFactory.getOptions()),
				namespace);
	}
}
