/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.operations;

import java.nio.ByteBuffer;
import java.util.Iterator;
import org.bouncycastle.util.Arrays;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.metadata.MetadataIterators;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public class CassandraMetadataReader implements MetadataReader {
  private final CassandraOperations operations;
  private final MetadataType metadataType;

  public CassandraMetadataReader(
      final CassandraOperations operations,
      final MetadataType metadataType) {
    this.operations = operations;
    this.metadataType = metadataType;
  }

  @Override
  public CloseableIterator<GeoWaveMetadata> query(final MetadataQuery query) {
    final String tableName = operations.getMetadataTableName(metadataType);
    String[] selectedColumns = getSelectedColumns(query);
    Predicate<Row> clientFilter = null;
    if (metadataType.isStatValues()) {
      selectedColumns = Arrays.append(selectedColumns, CassandraMetadataWriter.VISIBILITY_KEY);
    }
    if (query.isPrefix()) {
      if (query.hasPrimaryId()) {
        clientFilter = new PrimaryIDPrefixFilter(query.getPrimaryId());
      }
    }
    final Select select = operations.getSelect(tableName, selectedColumns);
    if (query.hasPrimaryId() && query.isExact()) {
      final Where where =
          select.where(
              QueryBuilder.eq(
                  CassandraMetadataWriter.PRIMARY_ID_KEY,
                  ByteBuffer.wrap(query.getPrimaryId())));
      if (query.hasSecondaryId()) {
        where.and(
            QueryBuilder.eq(
                CassandraMetadataWriter.SECONDARY_ID_KEY,
                ByteBuffer.wrap(query.getSecondaryId())));
      }
    } else if (query.hasSecondaryId()) {
      select.allowFiltering().where(
          QueryBuilder.eq(
              CassandraMetadataWriter.SECONDARY_ID_KEY,
              ByteBuffer.wrap(query.getSecondaryId())));
    }
    final ResultSet rs = operations.getSession().execute(select);
    final Iterator<Row> rows = rs.iterator();
    final CloseableIterator<GeoWaveMetadata> retVal =
        new CloseableIterator.Wrapper<>(
            Iterators.transform(
                clientFilter != null ? Iterators.filter(rows, clientFilter) : rows,
                result -> new GeoWaveMetadata(
                    (query.hasPrimaryId() && query.isExact()) ? query.getPrimaryId()
                        : result.get(
                            CassandraMetadataWriter.PRIMARY_ID_KEY,
                            ByteBuffer.class).array(),
                    useSecondaryId(query) ? query.getSecondaryId()
                        : result.get(
                            CassandraMetadataWriter.SECONDARY_ID_KEY,
                            ByteBuffer.class).array(),
                    getVisibility(query, result),
                    result.get(CassandraMetadataWriter.VALUE_KEY, ByteBuffer.class).array())));
    return query.getAuthorizations() != null
        ? MetadataIterators.clientVisibilityFilter(retVal, query.getAuthorizations())
        : retVal;
  }

  private byte[] getVisibility(final MetadataQuery query, final Row result) {
    if (metadataType.isStatValues()) {
      final ByteBuffer buf = result.get(CassandraMetadataWriter.VISIBILITY_KEY, ByteBuffer.class);
      if (buf != null) {
        return buf.array();
      }
    }
    return null;
  }

  private String[] getSelectedColumns(final MetadataQuery query) {
    if (query.hasPrimaryId() && query.isExact()) {
      if (useSecondaryId(query)) {
        return new String[] {CassandraMetadataWriter.VALUE_KEY};
      }

      return new String[] {
          CassandraMetadataWriter.SECONDARY_ID_KEY,
          CassandraMetadataWriter.VALUE_KEY};
    }
    if (useSecondaryId(query)) {
      return new String[] {
          CassandraMetadataWriter.PRIMARY_ID_KEY,
          CassandraMetadataWriter.VALUE_KEY};
    }
    return new String[] {
        CassandraMetadataWriter.PRIMARY_ID_KEY,
        CassandraMetadataWriter.SECONDARY_ID_KEY,
        CassandraMetadataWriter.VALUE_KEY};
  }

  private boolean useSecondaryId(final MetadataQuery query) {
    return !(MetadataType.STATISTICS.equals(metadataType)
        || MetadataType.STATISTIC_VALUES.equals(metadataType)
        || MetadataType.INTERNAL_ADAPTER.equals(metadataType)) || query.hasSecondaryId();
  }

  private static class PrimaryIDPrefixFilter implements Predicate<Row> {
    private final byte[] prefix;

    public PrimaryIDPrefixFilter(final byte[] prefix) {
      this.prefix = prefix;
    }

    @Override
    public boolean apply(final Row row) {
      if (row == null) {
        return false;
      }
      byte[] primaryId = row.get(CassandraMetadataWriter.PRIMARY_ID_KEY, ByteBuffer.class).array();
      return ByteArrayUtils.startsWith(primaryId, prefix);
    }
  }
}
