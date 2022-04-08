/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base.dataidx;

import org.locationtech.geowave.core.store.base.dataidx.DefaultDataIndexRowWriterWrapper.GeoWaveRowWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RowDeleter;

public class DefaultDataIndexRowDeleterWrapper implements RowDeleter {
  private final RowDeleter delegateDeleter;

  public DefaultDataIndexRowDeleterWrapper(final RowDeleter delegateDeleter) {
    this.delegateDeleter = delegateDeleter;
  }

  @Override
  public void delete(final GeoWaveRow row) {
    delegateDeleter.delete(new GeoWaveRowWrapper(row));
  }

  @Override
  public void flush() {
    delegateDeleter.flush();
  }

  @Override
  public void close() {
    delegateDeleter.close();
  }
}
