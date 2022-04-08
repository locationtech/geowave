/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.ingest.plugin;

import org.locationtech.geowave.adapter.vector.ingest.MinimalSimpleFeatureIngestFormat;
import org.locationtech.geowave.adapter.vector.ingest.MinimalSimpleFeatureIngestPlugin;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;

/**
 * In order for the custom ingest plugin to be usable via the GeoWave CLI, it must be registered as
 * an available format. This can be done by extending the {@link MinimalSimpleFeatureIngestFormat}
 * class and registering the new class.
 *
 * The ingest format can be registered by adding it to
 * `src/main/resources/META-INF/services/org.locationtech.geowave.core.ingest.spi.IngestFormatPluginProviderSpi`.
 */
public class CustomIngestFormat extends MinimalSimpleFeatureIngestFormat {

  @Override
  public String getIngestFormatName() {
    return "geonames";
  }

  @Override
  public String getIngestFormatDescription() {
    return "Example custom ingest format for geonames text file";
  }

  @Override
  protected MinimalSimpleFeatureIngestPlugin newPluginInstance(final IngestFormatOptions options) {
    return new CustomIngestPlugin();
  }

}
