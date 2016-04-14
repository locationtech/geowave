package mil.nga.giat.geowave.core.store.spi;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * These are the plugin index types that can be registered and used within
 * Geowave.
 */
public class DimensionalityTypeRegistry
{

	private final static Logger LOGGER = LoggerFactory.getLogger(DimensionalityTypeRegistry.class);

	private static Map<String, DimensionalityTypeProviderSpi> registeredDimensionalityTypes = null;
	private static String defaultDimensionalityType;

	private static synchronized void initDimensionalityTypeRegistry() {
		registeredDimensionalityTypes = new HashMap<String, DimensionalityTypeProviderSpi>();
		final Iterator<DimensionalityTypeProviderSpi> dimensionalityTypesProviders = ServiceLoader.load(
				DimensionalityTypeProviderSpi.class).iterator();
		int currentDefaultPriority = Integer.MIN_VALUE;
		while (dimensionalityTypesProviders.hasNext()) {
			final DimensionalityTypeProviderSpi dimensionalityTypeProvider = dimensionalityTypesProviders.next();
			if (registeredDimensionalityTypes.containsKey(dimensionalityTypeProvider.getDimensionalityTypeName())) {
				LOGGER.warn("Dimensionality type '" + dimensionalityTypeProvider.getDimensionalityTypeName() + "' already registered.  Unable to register type provided by " + dimensionalityTypeProvider.getClass().getName());
			}
			else {
				registeredDimensionalityTypes.put(
						dimensionalityTypeProvider.getDimensionalityTypeName(),
						dimensionalityTypeProvider);
				if (dimensionalityTypeProvider.getPriority() > currentDefaultPriority) {
					currentDefaultPriority = dimensionalityTypeProvider.getPriority();
					defaultDimensionalityType = dimensionalityTypeProvider.getDimensionalityTypeName();
				}
			}
		}
	}

	public static Map<String, DimensionalityTypeProviderSpi> getRegisteredDimensionalityTypes() {
		if (registeredDimensionalityTypes == null) {
			initDimensionalityTypeRegistry();
		}
		return Collections.unmodifiableMap(registeredDimensionalityTypes);
	}

	public static DimensionalityTypeProviderSpi getSelectedDimensionalityProvider(
			final String dimensionalityType ) {
		if (registeredDimensionalityTypes == null) {
			initDimensionalityTypeRegistry();
		}

		return registeredDimensionalityTypes.get(dimensionalityType);
	}

	public static String getDefaultDimensionalityType() {
		if (registeredDimensionalityTypes == null) {
			initDimensionalityTypeRegistry();
		}
		if (defaultDimensionalityType == null) {
			return "";
		}
		return defaultDimensionalityType;
	}
}
