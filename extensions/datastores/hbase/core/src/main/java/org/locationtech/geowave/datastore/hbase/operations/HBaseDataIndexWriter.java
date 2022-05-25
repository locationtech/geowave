/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.operations;

import java.io.IOException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseDataIndexWriter implements RowWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(HBaseWriter.class);

  private final BufferedMutator mutator;

  public HBaseDataIndexWriter(final BufferedMutator mutator) {
    this.mutator = mutator;
  }

  @Override
  public void close() {
    try {
      mutator.close();
    } catch (final IOException e) {
      LOGGER.warn("Unable to close BufferedMutator", e);
    }
  }

  @Override
  public void flush() {
    try {
      mutator.flush();
    } catch (final IOException e) {
      LOGGER.warn("Unable to flush BufferedMutator", e);
    }
  }

  @Override
  public void write(final GeoWaveRow[] rows) {
    for (final GeoWaveRow row : rows) {
      write(row);
    }
  }

  @Override
  public void write(final GeoWaveRow row) {
    writeMutations(rowToMutation(row));
  }

  private void writeMutations(final RowMutations rowMutation) {
    try {
      mutator.mutate(rowMutation.getMutations());
    } catch (final IOException e) {
      LOGGER.error("Unable to write mutation.", e);
    }
  }

  private RowMutations rowToMutation(final GeoWaveRow row) {
    final RowMutations mutation = new RowMutations(row.getDataId());
    for (final GeoWaveValue value : row.getFieldValues()) {
      final Put put = new Put(row.getDataId());
      // visibility is in the visibility column so no need to serialize it with the value
      put.addColumn(
          StringUtils.stringToBinary(ByteArrayUtils.shortToString(row.getAdapterId())),
          new byte[0],
          DataIndexUtils.serializeDataIndexValue(value, false));
      if ((value.getVisibility() != null) && (value.getVisibility().length > 0)) {
        put.setCellVisibility(
            new CellVisibility(StringUtils.stringFromBinary(value.getVisibility())));
      }
      try {
        mutation.add((Mutation) put);
      } catch (final IOException e) {
        LOGGER.error("Error creating HBase row mutation: " + e.getMessage());
      }
    }

    return mutation;
  }
}
