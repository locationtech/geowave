/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
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
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public class AccumuloDataIndexWriter extends AbstractAccumuloWriter {
  public AccumuloDataIndexWriter(
      final BatchWriter batchWriter,
      final AccumuloOperations operations,
      final String tableName) {
    super(batchWriter, operations, tableName);
  }

  public static Mutation rowToMutation(final GeoWaveRow row) {
    final Mutation mutation = new Mutation(row.getDataId());
    for (final GeoWaveValue value : row.getFieldValues()) {
      if ((value.getVisibility() != null) && (value.getVisibility().length > 0)) {
        mutation.put(
            new Text(ByteArrayUtils.shortToString(row.getAdapterId())),
            new Text(),
            new ColumnVisibility(value.getVisibility()),
            new Value(DataIndexUtils.serializeDataIndexValue(value, false)));
      } else {
        mutation.put(
            new Text(ByteArrayUtils.shortToString(row.getAdapterId())),
            new Text(),
            new Value(DataIndexUtils.serializeDataIndexValue(value, false)));
      }
    }
    return mutation;
  }

  @Override
  protected Mutation internalRowToMutation(final GeoWaveRow row) {
    return rowToMutation(row);
  }

}
