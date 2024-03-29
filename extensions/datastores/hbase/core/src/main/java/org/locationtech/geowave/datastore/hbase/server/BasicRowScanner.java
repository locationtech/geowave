/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.server;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;

public class BasicRowScanner implements RowScanner {

  private final List<Cell> list;
  private final Scan scan;
  private Map<String, Object> hints;

  public BasicRowScanner(final List<Cell> list, final Scan scan) {
    this.list = list;
    this.scan = scan;
  }

  @Override
  public boolean isMidRow() {
    return false;
  }

  @Override
  public List<Cell> nextCellsInRow() {
    return Collections.EMPTY_LIST;
  }

  @Override
  public boolean isDone() {
    return false;
  }

  @Override
  public List<Cell> currentCellsInRow() {
    return list;
  }

  @Override
  public Scan getScan() {
    return scan;
  }

  @Override
  public Map<String, Object> getHints() {
    if (hints == null) {
      // this isn't threadsafe but shouldn't need to be
      hints = new HashMap<>();
    }
    return hints;
  }
}
