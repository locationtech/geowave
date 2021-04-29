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
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CassandraMetadataDeleter implements MetadataDeleter {
  private final CassandraOperations operations;
  private final MetadataType metadataType;

  public CassandraMetadataDeleter(
      final CassandraOperations operations,
      final MetadataType metadataType) {
    this.operations = operations;
    this.metadataType = metadataType;
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean delete(final MetadataQuery query) {
    // deleting by secondary ID without primary ID is not supported (and not
    // directly supported by cassandra, we'd have to query first to get
    // primary ID(s) and then delete, but this is not a use case necessary
    // at the moment
    if (query.hasPrimaryId()) {
      final Delete delete = operations.getDelete(operations.getMetadataTableName(metadataType));
      final Where where =
          delete.where(
              QueryBuilder.eq(
                  CassandraMetadataWriter.PRIMARY_ID_KEY,
                  ByteBuffer.wrap(query.getPrimaryId())));
      if (query.hasSecondaryId()) {
        where.and(
            QueryBuilder.eq(
                CassandraMetadataWriter.SECONDARY_ID_KEY,
                ByteBuffer.wrap(query.getSecondaryId())));
      }
      operations.getSession().execute(delete);
    } else if (operations.getOptions().isVisibilityEnabled()) {
      // we need to respect visibilities although this may be much slower
      DataStoreUtils.safeMetadataDelete(this, operations, metadataType, query);
    } else {
      // without visibilities it is much faster to drop the table
      operations.dropMetadataTable(metadataType);
    }
    return true;
  }

  @Override
  public void flush() {
    // TODO Auto-generated method stub

  }
}
