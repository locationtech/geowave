package org.locationtech.geowave.core.store.ingest;

import java.util.List;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestUtils.class);

  public static boolean checkIndexesAgainstProvider(
      final String providerName,
      final DataAdapterProvider<?> adapterProvider,
      final List<IndexPluginOptions> indexOptions) {
    boolean valid = true;
    for (final IndexPluginOptions option : indexOptions) {
      if (!isCompatible(adapterProvider, option)) {
        // HP Fortify "Log Forging" false positive
        // What Fortify considers "user input" comes only
        // from users with OS-level access anyway
        LOGGER.warn(
            "Local file ingest plugin for ingest type '"
                + providerName
                + "' does not support dimensionality '"
                + option.getType()
                + "'");
        valid = false;
      }
    }
    return valid;
  }

  /**
   * Determine whether an index is compatible with the visitor
   *
   * @param index an index that an ingest type supports
   * @return whether the adapter is compatible with the common index model
   */
  public static boolean isCompatible(
      final DataAdapterProvider<?> adapterProvider,
      final IndexPluginOptions dimensionalityProvider) {
    final Class<? extends CommonIndexValue>[] supportedTypes =
        adapterProvider.getSupportedIndexableTypes();
    if ((supportedTypes == null) || (supportedTypes.length == 0)) {
      return false;
    }
    final Class<? extends CommonIndexValue>[] requiredTypes =
        dimensionalityProvider.getIndexPlugin().getRequiredIndexTypes();
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
}
