/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.filters;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.mapreduce.URLClassloaderUtils;

public class HBaseMergingFilter extends FilterBase {
  public HBaseMergingFilter() {}

  public static HBaseMergingFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
    final HBaseMergingFilter mergingFilter = new HBaseMergingFilter();

    return mergingFilter;
  }

  /** Enable filterRowCells */
  @Override
  public boolean hasFilterRow() {
    return true;
  }

  /** Handle the entire row at one time */
  @Override
  public void filterRowCells(final List<Cell> rowCells) throws IOException {
    if (!rowCells.isEmpty()) {
      if (rowCells.size() > 1) {
        try {
          final Cell firstCell = rowCells.get(0);
          final byte[] singleRow = CellUtil.cloneRow(firstCell);
          final byte[] singleFam = CellUtil.cloneFamily(firstCell);
          final byte[] singleQual = CellUtil.cloneQualifier(firstCell);

          Mergeable mergedValue = null;
          for (final Cell cell : rowCells) {
            final byte[] byteValue = CellUtil.cloneValue(cell);
            final Mergeable value = (Mergeable) URLClassloaderUtils.fromBinary(byteValue);

            if (mergedValue != null) {
              mergedValue.merge(value);
            } else {
              mergedValue = value;
            }
          }

          final Cell singleCell =
              CellUtil.createCell(
                  singleRow,
                  singleFam,
                  singleQual,
                  System.currentTimeMillis(),
                  KeyValue.Type.Put.getCode(),
                  URLClassloaderUtils.toBinary(mergedValue));

          rowCells.clear();
          rowCells.add(singleCell);
        } catch (final Exception e) {
          throw new IOException("Exception in filter", e);
        }
      }
    }
  }

  /** Don't do anything special here, since we're only interested in whole rows */
  @Override
  public ReturnCode filterKeyValue(final Cell cell) throws IOException {
    return ReturnCode.INCLUDE;
  }
}
