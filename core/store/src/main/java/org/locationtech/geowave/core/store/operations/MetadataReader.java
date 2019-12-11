/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

/**
 * Provides an interface for reading GeoWave metadata. A {@link MetadataQuery} is used to specify the metadata to be read.
 */
public interface MetadataReader {
  /**
   * Read metadata, as specified by the query.
   *
   * @param query The query that specifies the metadata to be read.
   * @return An iterator that lazily loads the metadata as they are requested.
   */
  CloseableIterator<GeoWaveMetadata> query(MetadataQuery query);
}
