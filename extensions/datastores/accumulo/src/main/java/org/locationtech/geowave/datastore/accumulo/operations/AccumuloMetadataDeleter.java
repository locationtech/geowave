/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.operations;

import java.util.Collections;
import java.util.List;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.accumulo.util.AccumuloUtils;

public class AccumuloMetadataDeleter implements MetadataDeleter {

  private final AccumuloOperations operations;
  private final String metadataTypeId;

  public AccumuloMetadataDeleter(
      final AccumuloOperations operations,
      final MetadataType metadataType) {
    super();
    this.operations = operations;
    metadataTypeId = metadataType.id();
  }

  @Override
  public void close() throws Exception {}

  @Override
  public boolean delete(final MetadataQuery query) {
    if (query.hasPrimaryIdRanges() || (query.hasPrimaryId() && query.isPrefix())) {
      final List<Range> ranges =
          query.hasPrimaryIdRanges()
              ? AccumuloUtils.byteArrayRangesToBoundedAccumuloRanges(query.getPrimaryIdRanges())
              : Collections.singletonList(Range.prefix(new Text(query.getPrimaryId())));
      if (ranges.isEmpty()) {
        return false;
      }
      return operations.deleteRanges(
          AbstractGeoWavePersistence.METADATA_TABLE,
          ranges,
          metadataTypeId,
          query.getSecondaryId(),
          query.getAuthorizations());
    }
    // the nature of metadata deleter is that primary ID is always
    // well-defined and it is deleting a single entry at a time
    return operations.delete(
        AbstractGeoWavePersistence.METADATA_TABLE,
        new ByteArray(query.getPrimaryId()),
        metadataTypeId,
        query.getSecondaryId() != null ? query.getSecondaryId() : null,
        query.getAuthorizations());
  }

  @Override
  public void flush() {}
}
