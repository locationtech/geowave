/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.server;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

public class ServerOpRegionScannerWrapper extends ServerOpInternalScannerWrapper implements
    RegionScanner {
  public ServerOpRegionScannerWrapper(
      final Collection<HBaseServerOp> orderedServerOps,
      final RegionScanner delegate,
      final Scan scan) {
    super(orderedServerOps, delegate, scan);
  }

  @Override
  public RegionInfo getRegionInfo() {
    return ((RegionScanner) delegate).getRegionInfo();
  }

  @Override
  public boolean isFilterDone() throws IOException {
    return ((RegionScanner) delegate).isFilterDone();
  }

  @Override
  public boolean reseek(final byte[] row) throws IOException {
    return ((RegionScanner) delegate).reseek(row);
  }

  @Override
  public long getMaxResultSize() {
    return ((RegionScanner) delegate).getMaxResultSize();
  }

  @Override
  public long getMvccReadPoint() {
    return ((RegionScanner) delegate).getMvccReadPoint();
  }

  @Override
  public int getBatch() {
    return ((RegionScanner) delegate).getBatch();
  }

  @Override
  public boolean nextRaw(final List<Cell> rowCells) throws IOException {
    final boolean retVal = ((RegionScanner) delegate).nextRaw(rowCells);
    if (!internalNextRow(rowCells)) {
      return false;
    }
    return retVal;
  }

  @Override
  public boolean nextRaw(final List<Cell> rowCells, final ScannerContext scannerContext)
      throws IOException {
    final boolean retVal = ((RegionScanner) delegate).nextRaw(rowCells, scannerContext);
    if (!internalNextRow(rowCells, scannerContext)) {
      return false;
    }
    return retVal;
  }
}
