package mil.nga.giat.geowave.core.ingest.spi;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import mil.nga.giat.geowave.core.store.config.ConfigUtils;

public class IngestFormatPluginRegistry
{

	private static Map<String, IngestFormatPluginProviderSpi<?, ?>> pluginProviderRegistry = null;

	private IngestFormatPluginRegistry() {}

	@SuppressWarnings("rawtypes")
	private static void initPluginProviderRegistry() {
		pluginProviderRegistry = new HashMap<String, IngestFormatPluginProviderSpi<?, ?>>();
		final Iterator<IngestFormatPluginProviderSpi> pluginProviders = ServiceLoader.load(
				IngestFormatPluginProviderSpi.class).iterator();
		while (pluginProviders.hasNext()) {
			final IngestFormatPluginProviderSpi pluginProvider = pluginProviders.next();
			pluginProviderRegistry.put(
					ConfigUtils.cleanOptionName(pluginProvider.getIngestFormatName()),
					pluginProvider);
		}
	}

	public static Map<String, IngestFormatPluginProviderSpi<?, ?>> getPluginProviderRegistry() {
		if (pluginProviderRegistry == null) {
			initPluginProviderRegistry();
		}
		return pluginProviderRegistry;
	}
}
