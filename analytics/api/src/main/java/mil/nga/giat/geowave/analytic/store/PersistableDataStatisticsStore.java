package mil.nga.giat.geowave.analytic.store;

import java.util.Map;

import mil.nga.giat.geowave.core.cli.DataStatisticsStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;

public class PersistableDataStatisticsStore extends
		PersistableStore<DataStatisticsStore>
{
	public PersistableDataStatisticsStore() {
		super();
	}

	public PersistableDataStatisticsStore(
			final GenericStoreCommandLineOptions<DataStatisticsStore> cliOptions ) {
		super(
				cliOptions);
	}

	@Override
	protected GenericStoreCommandLineOptions<DataStatisticsStore> getCLIOptions(
			final Map<String, String> configOptions,
			final String namespace,
			final String factoryName ) {
		final DataStatisticsStoreFactorySpi statisticsStoreFactory = GeoWaveStoreFinder.getRegisteredDataStatisticsStoreFactories().get(
				factoryName);
		return new DataStatisticsStoreCommandLineOptions(
				statisticsStoreFactory,
				ConfigUtils.valuesFromStrings(
						configOptions,
						statisticsStoreFactory.getOptions()),
				namespace);
	}
}
