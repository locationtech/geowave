/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.iterators;

import java.io.IOException;
import java.util.Collection;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;

public abstract class ExceptionHandlingFilter extends Filter {

  @Override
  public final boolean accept(Key k, Value v) {
    try {
      return acceptInternal(k, v);
    } catch (Exception e) {
      throw new WrappingFilterException("Exception in filter.", e);
    }
  }

  protected abstract boolean acceptInternal(Key k, Value v);

  @Override
  public void next() throws IOException {
    try {
      super.next();
    } catch (WrappingFilterException e) {
      throw new IOException(e.getCause());
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    try {
      super.seek(range, columnFamilies, inclusive);
    } catch (WrappingFilterException e) {
      throw new IOException(e.getCause());
    }
  }

  private static class WrappingFilterException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public WrappingFilterException(String message, Exception e) {
      super(message, e);
    }
  }

}
