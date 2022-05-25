/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.operations;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractAccumuloWriter implements RowWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAccumuloWriter.class);
  private org.apache.accumulo.core.client.BatchWriter batchWriter;
  private final AccumuloOperations operations;
  private final String tableName;

  public AbstractAccumuloWriter(
      final org.apache.accumulo.core.client.BatchWriter batchWriter,
      final AccumuloOperations operations,
      final String tableName) {
    this.batchWriter = batchWriter;
    this.operations = operations;
    this.tableName = tableName;
  }

  public org.apache.accumulo.core.client.BatchWriter getBatchWriter() {
    return batchWriter;
  }

  public void setBatchWriter(final org.apache.accumulo.core.client.BatchWriter batchWriter) {
    this.batchWriter = batchWriter;
  }

  public void write(final Iterable<Mutation> mutations) {
    try {
      batchWriter.addMutations(mutations);
    } catch (final MutationsRejectedException e) {
      LOGGER.error("Unable to close batch writer", e);
    }
  }

  public void write(final Mutation mutation) {
    try {
      batchWriter.addMutation(mutation);
    } catch (final MutationsRejectedException e) {
      LOGGER.error("Unable to write batch writer", e);
    }
  }

  @Override
  public void close() {
    try {
      batchWriter.close();
    } catch (final MutationsRejectedException e) {
      LOGGER.error("Unable to close batch writer", e);
    }
  }

  @Override
  public void flush() {
    try {
      batchWriter.flush();
    } catch (final MutationsRejectedException e) {
      LOGGER.error("Unable to flush batch writer", e);
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
    final byte[] partition = row.getPartitionKey();
    if ((partition != null) && (partition.length > 0)) {
      operations.ensurePartition(new ByteArray(partition), tableName);
    }
    write(internalRowToMutation(row));
  }

  abstract protected Mutation internalRowToMutation(final GeoWaveRow row);
}
