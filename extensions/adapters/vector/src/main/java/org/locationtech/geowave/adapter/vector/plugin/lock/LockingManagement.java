/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin.lock;

import org.geotools.data.LockingManager;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.index.LockManager;

/**
 * An extension to {@link LockManager} to support requesting a lock on a specific feature under a
 * provided transaction. Implementers must check transaction state as AUTO_COMMIT. Locking under an
 * AUTO_COMMIT is not authorized.
 */
public interface LockingManagement extends LockingManager {

  /**
   * Lock a feature for a provided transaction. This is typically used for modifications (updates).
   *
   * @param transaction
   * @param featureID
   */
  public void lock(Transaction transaction, String featureID);
}
