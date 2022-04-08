/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseMetadataWriter implements MetadataWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(HBaseMetadataWriter.class);

  private final BufferedMutator writer;
  protected Set<ByteArray> duplicateRowTracker = new HashSet<>();
  private final byte[] metadataTypeBytes;

  public HBaseMetadataWriter(final BufferedMutator writer, final MetadataType metadataType) {
    this.writer = writer;
    metadataTypeBytes = StringUtils.stringToBinary(metadataType.id());
  }

  @Override
  public void close() throws Exception {
    try {
      synchronized (duplicateRowTracker) {
        safeFlush();
        writer.close();
      }
    } catch (final IOException e) {
      LOGGER.warn("Unable to close metadata writer", e);
    }
  }

  @Override
  public void flush() {
    try {
      synchronized (duplicateRowTracker) {
        safeFlush();
      }
    } catch (final IOException e) {
      LOGGER.warn("Unable to flush metadata writer", e);
    }
  }

  private long lastFlush = -1;

  @Override
  public void write(final GeoWaveMetadata metadata) {

    // we use a hashset of row IDs so that we can retain multiple versions
    // (otherwise timestamps will be applied on the server side in
    // batches and if the same row exists within a batch we will not
    // retain multiple versions)
    final Put put = new Put(metadata.getPrimaryId());

    final byte[] secondaryBytes =
        metadata.getSecondaryId() != null ? metadata.getSecondaryId() : new byte[0];

    put.addColumn(metadataTypeBytes, secondaryBytes, metadata.getValue());

    if ((metadata.getVisibility() != null) && (metadata.getVisibility().length > 0)) {
      put.setCellVisibility(
          new CellVisibility(StringUtils.stringFromBinary(metadata.getVisibility())));
    }

    try {
      synchronized (duplicateRowTracker) {
        final ByteArray primaryId = new ByteArray(metadata.getPrimaryId());
        if (!duplicateRowTracker.add(primaryId)) {
          safeFlush();
          duplicateRowTracker.add(primaryId);
        }
      }
      writer.mutate(put);
    } catch (final IOException e) {
      LOGGER.error("Unable to write metadata", e);
    }
  }

  private void safeFlush() throws IOException {
    while (System.currentTimeMillis() <= lastFlush) {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
        LOGGER.warn("Unable to wait for new time", e);
      }
    }
    writer.flush();
    lastFlush = System.currentTimeMillis();
    duplicateRowTracker.clear();
  }
}
