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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

public class ServerOpInternalScannerWrapper implements InternalScanner {
  protected final Collection<HBaseServerOp> orderedServerOps;
  protected InternalScanner delegate;
  protected Scan scan;

  public ServerOpInternalScannerWrapper(
      final Collection<HBaseServerOp> orderedServerOps,
      final InternalScanner delegate,
      final Scan scan) {
    this.orderedServerOps = orderedServerOps;
    this.delegate = delegate;
    this.scan = scan;
  }

  protected boolean internalNextRow(final RowScanner rowScanner) throws IOException {
    for (final HBaseServerOp serverOp : orderedServerOps) {
      if (!serverOp.nextRow(rowScanner)) {
        return false;
      }
    }
    return !rowScanner.isDone();
  }

  protected boolean internalNextRow(final List<Cell> rowCells) throws IOException {
    return internalNextRow(new BasicRowScanner(rowCells, scan));
  }

  protected boolean internalNextRow(final List<Cell> rowCells, final ScannerContext scannerContext)
      throws IOException {
    return internalNextRow(rowCells);
  }

  @Override
  public boolean next(final List<Cell> rowCells) throws IOException {
    final boolean retVal = delegate.next(rowCells);
    if (!internalNextRow(rowCells)) {
      return false;
    }
    return retVal;
  }

  @Override
  public boolean next(final List<Cell> rowCells, final ScannerContext scannerContext)
      throws IOException {
    final boolean retVal = delegate.next(rowCells, scannerContext);
    if (!internalNextRow(rowCells, scannerContext)) {
      return false;
    }
    return retVal;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
