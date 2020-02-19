/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.tdrive;

import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestFormat;
import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;

/**
 * This represents an ingest format plugin provider for GPX data. It will support ingesting directly
 * from a local file system or staging data from a local files system and ingesting into GeoWave
 * using a map-reduce job.
 */
public class TdriveIngestFormat extends AbstractSimpleFeatureIngestFormat<AvroTdrivePoint> {
  @Override
  protected AbstractSimpleFeatureIngestPlugin<AvroTdrivePoint> newPluginInstance(
      final IngestFormatOptions options) {
    return new TdriveIngestPlugin();
  }

  @Override
  public String getIngestFormatName() {
    return "tdrive";
  }

  @Override
  public String getIngestFormatDescription() {
    return "files from Microsoft Research T-Drive trajectory data set";
  }
}
