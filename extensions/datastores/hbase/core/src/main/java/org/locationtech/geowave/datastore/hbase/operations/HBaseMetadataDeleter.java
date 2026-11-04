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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseMetadataDeleter implements MetadataDeleter {
  private static final Logger LOGGER = LoggerFactory.getLogger(HBaseMetadataDeleter.class);

  private static final int BATCH_SIZE = 1000;

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
    final TableName tableName =
        operations.getTableName(operations.getMetadataTableName(metadataType));
    final byte[] columnFamily = StringUtils.stringToBinary(metadataType.id());
    if (query.hasPrimaryId() && query.isExact() && query.hasSecondaryId()) {
      try (final BufferedMutator deleter = operations.getBufferedMutator(tableName)) {
        final Delete delete = new Delete(query.getPrimaryId());
        delete.addColumns(columnFamily, query.getSecondaryId());
        deleter.mutate(delete);
        return true;
      } catch (final IOException e) {
        LOGGER.error("Error deleting metadata", e);
      }
      return false;
    }
    return bulkDelete(query, tableName, columnFamily);
  }

  // Scans only the keys of the matching cells, so values are never transferred or merged.
  private boolean bulkDelete(
      final MetadataQuery query,
      final TableName tableName,
      final byte[] columnFamily) {
    final Scan scan = new HBaseMetadataReader(operations, null, metadataType).createRowScan(query);
    if (scan == null) {
      return false;
    }
    scan.setFilter(
        scan.getFilter() == null ? new KeyOnlyFilter()
            : new FilterList(scan.getFilter(), new KeyOnlyFilter()));
    final String[] authorizations = query.getAuthorizations();
    if ((authorizations != null) && (authorizations.length > 0)) {
      scan.setAuthorizations(new Authorizations(authorizations));
    }
    long deleted = 0;
    Iterable<Result> results = null;
    try (final BufferedMutator deleter = operations.getBufferedMutator(tableName)) {
      results = operations.getScannedResults(scan, operations.getMetadataTableName(metadataType));
      List<Delete> batch = new ArrayList<>();
      for (final Result result : results) {
        for (final Cell cell : result.rawCells()) {
          final Delete delete = new Delete(CellUtil.cloneRow(cell));
          delete.addColumns(columnFamily, CellUtil.cloneQualifier(cell));
          batch.add(delete);
        }
        if (batch.size() >= BATCH_SIZE) {
          deleter.mutate(batch);
          deleted += batch.size();
          batch = new ArrayList<>();
        }
      }
      if (!batch.isEmpty()) {
        deleter.mutate(batch);
        deleted += batch.size();
      }
    } catch (final IOException | UncheckedIOException e) {
      // a mid-scan failure surfaces unchecked, and the batches already mutated still flush
      LOGGER.error(
          "Partial delete of metadata: the scan or a write failed after "
              + deleted
              + " deleted columns were submitted",
          e);
    } finally {
      if (results instanceof ResultScanner) {
        ((ResultScanner) results).close();
      }
    }
    return deleted > 0;
  }

  @Override
  public void flush() {}
}
