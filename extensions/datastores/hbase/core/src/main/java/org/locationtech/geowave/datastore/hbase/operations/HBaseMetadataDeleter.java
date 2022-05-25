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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseMetadataDeleter implements MetadataDeleter {
  private static final Logger LOGGER = LoggerFactory.getLogger(HBaseMetadataDeleter.class);

  private final HBaseOperations operations;
  private final MetadataType metadataType;

  public HBaseMetadataDeleter(final HBaseOperations operations, final MetadataType metadataType) {
    super();
    this.operations = operations;
    this.metadataType = metadataType;
  }

  @Override
  public void close() throws Exception {
    // TODO when stats merging happens on compaction with serverside libraries we can only sleep
    // when
    // not using serverside libraries, but thats not the case currently so we sleep all the time
    // if (!operations.isServerSideLibraryEnabled()) {
    // updates can happen with a delete immediately followed by an add, and in particular merging
    // stats without serverside libraries is always this case, so we need to make sure the delete
    // tombstone has an earlier timestamp than the subsequent add
    Thread.sleep(1);
    // }
  }

  @Override
  public boolean delete(final MetadataQuery query) {
    // the nature of metadata deleter is that primary ID is always
    // well-defined and it is deleting a single entry at a time
    final TableName tableName =
        operations.getTableName(operations.getMetadataTableName(metadataType));
    if (!query.hasPrimaryId() && !query.hasSecondaryId()) {
      // bulk delete should be much faster
      final MetadataReader reader = operations.createMetadataReader(metadataType);
      final List<Delete> listOfBatchDelete = new ArrayList<>();
      try (final CloseableIterator<GeoWaveMetadata> it = reader.query(query)) {
        while (it.hasNext()) {
          final GeoWaveMetadata entry = it.next();
          final Delete delete = new Delete(entry.getPrimaryId());
          delete.addColumns(StringUtils.stringToBinary(metadataType.id()), entry.getSecondaryId());
          listOfBatchDelete.add(delete);
        }
      }
      try {
        final BufferedMutator deleter = operations.getBufferedMutator(tableName);
        deleter.mutate(listOfBatchDelete);
        deleter.close();
        return true;
      } catch (final IOException e) {
        LOGGER.error("Error bulk deleting metadata", e);
      }
      return false;
    } else {
      try {
        final BufferedMutator deleter = operations.getBufferedMutator(tableName);

        final Delete delete = new Delete(query.getPrimaryId());
        delete.addColumns(StringUtils.stringToBinary(metadataType.id()), query.getSecondaryId());

        deleter.mutate(delete);
        deleter.close();

        return true;
      } catch (final IOException e) {
        LOGGER.error("Error deleting metadata", e);
      }
    }
    return false;
  }

  @Override
  public void flush() {}
}
