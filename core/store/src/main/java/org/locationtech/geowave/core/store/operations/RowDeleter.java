/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import java.io.Closeable;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

/**
 * Provides an interface for deleting GeoWave data rows.
 */
public interface RowDeleter extends Closeable {
  /**
   * Delete a GeoWave row from the DB.
   *
   * Preconditions: <ul> <li>The deleter is not closed</li> </ul>
   *
   * @param row The row to delete.
   */
  void delete(GeoWaveRow row);

  /**
   * Flush the deleter, committing all pending changes. Note that the changes may already be
   * committed - this method just establishes that they *must* be committed after the method
   * returns.
   *
   * Preconditions: <ul> <li>The deleter is not closed</li> </ul>
   */
  void flush();
}
