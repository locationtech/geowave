/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.operations;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

/**
 * This is a basic wrapper around the Accumulo batch writer so that write operations will use an
 * interface that can be implemented differently for different purposes. For example, a bulk ingest
 * can be performed by replacing this implementation within a custom implementation of
 * AccumuloOperations.
 */
public class AccumuloWriter extends AbstractAccumuloWriter {
  public AccumuloWriter(
      final BatchWriter batchWriter,
      final AccumuloOperations operations,
      final String tableName) {
    super(batchWriter, operations, tableName);
  }

  public static Mutation rowToMutation(final GeoWaveRow row) {
    final Mutation mutation = new Mutation(GeoWaveKey.getCompositeId(row));
    for (final GeoWaveValue value : row.getFieldValues()) {
      if ((value.getVisibility() != null) && (value.getVisibility().length > 0)) {
        mutation.put(
            new Text(ByteArrayUtils.shortToString(row.getAdapterId())),
            new Text(value.getFieldMask()),
            new ColumnVisibility(value.getVisibility()),
            new Value(value.getValue()));
      } else {
        mutation.put(
            new Text(ByteArrayUtils.shortToString(row.getAdapterId())),
            new Text(value.getFieldMask()),
            new Value(value.getValue()));
      }
    }
    return mutation;
  }

  @Override
  protected Mutation internalRowToMutation(final GeoWaveRow row) {
    return rowToMutation(row);
  }
}
