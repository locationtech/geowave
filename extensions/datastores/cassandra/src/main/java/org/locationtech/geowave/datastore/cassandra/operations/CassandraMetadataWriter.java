/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.operations;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;

public class CassandraMetadataWriter implements MetadataWriter {
  protected static final String PRIMARY_ID_KEY = "i";
  protected static final String SECONDARY_ID_KEY = "s";
  // serves as unique ID for instances where primary+secondary are repeated
  protected static final String TIMESTAMP_ID_KEY = "t";
  protected static final String VISIBILITY_KEY = "a";
  protected static final String VALUE_KEY = "v";

  private final CassandraOperations operations;
  private final String tableName;

  public CassandraMetadataWriter(final CassandraOperations operations, final String tableName) {
    this.operations = operations;
    this.tableName = tableName;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public void write(final GeoWaveMetadata metadata) {
    RegularInsert insert =
        operations.getInsert(tableName).value(
            PRIMARY_ID_KEY,
            QueryBuilder.literal(ByteBuffer.wrap(metadata.getPrimaryId())));
    if (metadata.getSecondaryId() != null) {
      insert =
          insert.value(
              SECONDARY_ID_KEY,
              QueryBuilder.literal(ByteBuffer.wrap(metadata.getSecondaryId()))).value(
                  TIMESTAMP_ID_KEY,
                  QueryBuilder.now());
      if ((metadata.getVisibility() != null) && (metadata.getVisibility().length > 0)) {
        insert =
            insert.value(
                VISIBILITY_KEY,
                QueryBuilder.literal(ByteBuffer.wrap(metadata.getVisibility())));
      }
    }

    insert = insert.value(VALUE_KEY, QueryBuilder.literal(ByteBuffer.wrap(metadata.getValue())));
    operations.getSession().execute(insert.build());
  }

  @Override
  public void flush() {}
}
