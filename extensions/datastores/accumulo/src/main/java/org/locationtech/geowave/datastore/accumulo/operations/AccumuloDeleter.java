/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.operations;

import java.util.List;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.operations.Deleter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloDeleter<T> extends AccumuloReader<T> implements Deleter<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloOperations.class);

  private boolean closed = false;

  public AccumuloDeleter(
      final BatchDeleter scanner,
      final List<ByteArrayRange> clientFilterRanges,
      final GeoWaveRowIteratorTransformer<T> transformer,
      final int partitionKeyLength,
      final boolean wholeRowEncoding,
      final boolean clientSideRowMerging,
      final boolean parallel) {
    super(
        scanner,
        clientFilterRanges,
        transformer,
        partitionKeyLength,
        wholeRowEncoding,
        clientSideRowMerging,
        parallel);
  }

  @Override
  public void close() {
    if (!closed) {
      // make sure delete is only called once
      try {
        ((BatchDeleter) scanner).delete();
      } catch (MutationsRejectedException | TableNotFoundException e) {
        LOGGER.error("Unable to delete row", e);
      }

      closed = true;
    }
    super.close();

  }


  @Override
  public void entryScanned(final T entry, final GeoWaveRow row) {}
}
