/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.Deleter;
import org.locationtech.geowave.core.store.operations.ReaderParams;

public class HBaseDeleter<T> extends HBaseReader<T> implements Deleter<T> {
  private boolean closed = false;

  public HBaseDeleter(final ReaderParams<T> readerParams, final HBaseOperations operations) {
    super(readerParams, operations);
  }

  @Override
  public void close() {
    if (!closed) {
      // make sure delete is only called once
      operations.bulkDelete(readerParams);

      closed = true;
    }
    super.close();
  }

  @Override
  public void entryScanned(final T entry, final GeoWaveRow row) {}
}
