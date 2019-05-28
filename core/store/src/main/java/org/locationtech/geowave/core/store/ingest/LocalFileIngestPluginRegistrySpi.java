package org.locationtech.geowave.core.store.ingest;

import java.util.Map;

public interface LocalFileIngestPluginRegistrySpi {
  Map<String, LocalFileIngestPlugin<?>> getDefaultLocalIngestPlugins();
}
