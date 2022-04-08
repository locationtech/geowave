/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.memory;

import java.util.Arrays;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataQuery;

/**
 * Filters memory metadata based on the given metadata query.
 */
public class MemoryMetadataFilteringIterator implements CloseableIterator<GeoWaveMetadata> {

  private final CloseableIterator<GeoWaveMetadata> source;
  private final MetadataQuery query;
  private final boolean hasSecondaryId;

  private GeoWaveMetadata next = null;

  public MemoryMetadataFilteringIterator(
      final CloseableIterator<GeoWaveMetadata> source,
      final MetadataQuery query) {
    this.source = source;
    this.query = query;
    this.hasSecondaryId = query.getSecondaryId() != null;
  }

  private boolean secondaryIdMatches(final GeoWaveMetadata metadata) {
    return !hasSecondaryId || Arrays.equals(metadata.getSecondaryId(), query.getSecondaryId());
  }

  private boolean passesExactFilter(final GeoWaveMetadata metadata) {
    return (!query.hasPrimaryId() || Arrays.equals(metadata.getPrimaryId(), query.getPrimaryId()))
        && secondaryIdMatches(metadata);
  }

  private boolean passesPrefixFilter(final GeoWaveMetadata metadata) {
    return (!query.hasPrimaryId()
        || ByteArrayUtils.startsWith(metadata.getPrimaryId(), query.getPrimaryId()))
        && secondaryIdMatches(metadata);
  }

  private void computeNext() {
    while (next == null && source.hasNext()) {
      GeoWaveMetadata possibleNext = source.next();
      if (query.isPrefix()) {
        if (passesPrefixFilter(possibleNext)) {
          next = possibleNext;
        }
      } else if (passesExactFilter(possibleNext)) {
        next = possibleNext;
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (next == null) {
      computeNext();
    }
    return next != null;
  }

  @Override
  public GeoWaveMetadata next() {
    if (next == null) {
      computeNext();
    }
    GeoWaveMetadata retVal = next;
    next = null;
    return retVal;
  }

  @Override
  public void close() {
    source.close();
  }

}
