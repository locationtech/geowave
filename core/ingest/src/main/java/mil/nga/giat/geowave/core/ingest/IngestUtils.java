package mil.nga.giat.geowave.core.ingest;

import java.util.List;

import mil.nga.giat.geowave.core.store.cli.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

public class IngestUtils
{
	/**
	 * Determine whether an index is compatible with the visitor
	 * 
	 * @param index
	 *            an index that an ingest type supports
	 * @return whether the adapter is compatible with the common index model
	 */
	public static boolean isCompatible(
			final DataAdapterProvider<?> adapterProvider,
			final IndexPluginOptions dimensionalityProvider ) {
		final Class<? extends CommonIndexValue>[] supportedTypes = adapterProvider.getSupportedIndexableTypes();
		if ((supportedTypes == null) || (supportedTypes.length == 0)) {
			return false;
		}
		final Class<? extends CommonIndexValue>[] requiredTypes = dimensionalityProvider
				.getIndexPlugin()
				.getRequiredIndexTypes();
		for (final Class<? extends CommonIndexValue> requiredType : requiredTypes) {
			boolean fieldFound = false;
			for (final Class<? extends CommonIndexValue> supportedType : supportedTypes) {
				if (requiredType.isAssignableFrom(supportedType)) {
					fieldFound = true;
					break;
				}
			}
			if (!fieldFound) {
				return false;
			}
		}
		return true;

	}

	public static boolean isSupported(
			final DataAdapterProvider<?> adapterProvider,
			final List<IndexPluginOptions> dimensionalityTypes ) {
		for (final IndexPluginOptions option : dimensionalityTypes) {
			if (isCompatible(
					adapterProvider,
					option)) {
				return true;
			}
		}
		return false;

	}

}
