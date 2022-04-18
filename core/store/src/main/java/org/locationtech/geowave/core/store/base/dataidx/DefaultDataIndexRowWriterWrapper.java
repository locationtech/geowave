/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base.dataidx;

import java.util.Arrays;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import com.google.common.primitives.Bytes;

public class DefaultDataIndexRowWriterWrapper implements RowWriter {
  private final RowWriter delegateWriter;

  public DefaultDataIndexRowWriterWrapper(final RowWriter delegateWriter) {
    this.delegateWriter = delegateWriter;
  }

  @Override
  public void close() throws Exception {
    delegateWriter.close();
  }

  @Override
  public void write(final GeoWaveRow[] rows) {
    Arrays.stream(rows).forEach(r -> delegateWriter.write(new GeoWaveRowWrapper(r)));
  }

  @Override
  public void write(final GeoWaveRow row) {
    delegateWriter.write(row);
  }

  @Override
  public void flush() {
    delegateWriter.flush();
  }

  protected static class GeoWaveRowWrapper implements GeoWaveRow {
    private final GeoWaveRow row;

    protected GeoWaveRowWrapper(final GeoWaveRow row) {
      this.row = row;
    }

    @Override
    public GeoWaveValue[] getFieldValues() {
      return row.getFieldValues();
    }

    @Override
    public byte[] getDataId() {
      return row.getDataId();
    }

    @Override
    public short getAdapterId() {
      return row.getAdapterId();
    }

    @Override
    public byte[] getSortKey() {
      final byte[] sortKey = row.getDataId();
      return Bytes.concat(new byte[] {(byte) sortKey.length}, sortKey);
    }

    @Override
    public byte[] getPartitionKey() {
      return row.getPartitionKey();
    }

    @Override
    public int getNumberOfDuplicates() {
      return row.getNumberOfDuplicates();
    }

  }
}
