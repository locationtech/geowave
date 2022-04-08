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
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;

public interface RowScanner {
  public boolean isMidRow();

  public List<Cell> nextCellsInRow() throws IOException;

  public boolean isDone();

  public List<Cell> currentCellsInRow();

  public Scan getScan();

  public Map<String, Object> getHints();
}
