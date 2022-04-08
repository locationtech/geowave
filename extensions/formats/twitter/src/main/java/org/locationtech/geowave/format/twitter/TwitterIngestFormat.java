/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.twitter;

import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestFormat;
import org.locationtech.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import org.locationtech.geowave.core.ingest.avro.AvroWholeFile;
import org.locationtech.geowave.core.store.ingest.IngestFormatOptions;

/**
 * This represents an ingest format plugin provider for Twitter data. It will support ingesting
 * directly from a local file system or staging data from a local files system and ingesting into
 * GeoWave using a map-reduce job.
 */
public class TwitterIngestFormat extends AbstractSimpleFeatureIngestFormat<AvroWholeFile> {

  @Override
  protected AbstractSimpleFeatureIngestPlugin<AvroWholeFile> newPluginInstance(
      final IngestFormatOptions options) {
    return new TwitterIngestPlugin();
  }

  @Override
  public String getIngestFormatName() {
    return "twitter";
  }

  @Override
  public String getIngestFormatDescription() {
    return "Flattened compressed files from Twitter API";
  }
}
