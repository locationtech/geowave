/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;

public abstract class ExceptionHandlingFilter extends Filter {

  @Override
  public final boolean accept(final Key k, final Value v) {
    try {
      return acceptInternal(k, v);
    } catch (final Exception e) {
      throw new ServerSideIteratorException("Exception in filter", e);
    }
  }

  protected abstract boolean acceptInternal(Key k, Value v);
}
