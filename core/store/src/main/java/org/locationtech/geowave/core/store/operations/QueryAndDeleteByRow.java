/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import java.util.NoSuchElementException;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class QueryAndDeleteByRow<T> implements Deleter<T> {
  private final RowDeleter rowDeleter;
  private final RowReader<T> reader;

  public QueryAndDeleteByRow() {
    this.reader = new EmptyReader<>();
    rowDeleter = null;
  }

  public QueryAndDeleteByRow(final RowDeleter rowDeleter, final RowReader<T> reader) {
    this.rowDeleter = rowDeleter;
    this.reader = reader;
  }

  @Override
  public void close() {
    reader.close();
    rowDeleter.close();
  }

  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  @Override
  public T next() {
    return reader.next();
  }

  @Override
  public void entryScanned(final T entry, final GeoWaveRow row) {
    rowDeleter.delete(row);
  }

  private static class EmptyReader<T> implements RowReader<T> {

    @Override
    public void close() {}

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public T next() {
      throw new NoSuchElementException();
    }
  }
}
