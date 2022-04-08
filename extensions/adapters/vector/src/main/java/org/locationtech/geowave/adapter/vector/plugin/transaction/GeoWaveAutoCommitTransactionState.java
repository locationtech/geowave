/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin.transaction;

import java.io.IOException;
import org.geotools.data.Transaction;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;
import org.locationtech.geowave.adapter.vector.plugin.GeoWaveFeatureSource;

public class GeoWaveAutoCommitTransactionState implements GeoWaveTransactionState {

  private final GeoWaveDataStoreComponents components;

  public GeoWaveAutoCommitTransactionState(final GeoWaveFeatureSource source) {
    components = source.getComponents();
  }

  @Override
  public void setTransaction(final Transaction transaction) {}

  /** @see org.geotools.data.Transaction.State#addAuthorization(java.lang.String) */
  @Override
  public void addAuthorization(final String AuthID) throws IOException {
    // not required for
  }

  /**
   * Will apply differences to store.
   *
   * @see org.geotools.data.Transaction.State#commit()
   */
  @Override
  public void commit() throws IOException {
    // not required for
  }

  /** @see org.geotools.data.Transaction.State#rollback() */
  @Override
  public void rollback() throws IOException {}

  @Override
  public GeoWaveTransaction getGeoWaveTransaction(final String typeName) {
    // TODO Auto-generated method stub
    return new GeoWaveEmptyTransaction(components);
  }

  @Override
  public String toString() {
    return "GeoWaveAutoCommitTransactionState";
  }
}
