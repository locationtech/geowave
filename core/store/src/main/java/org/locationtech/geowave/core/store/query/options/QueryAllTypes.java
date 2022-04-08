/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.options;

public class QueryAllTypes<T> extends FilterByTypeQueryOptions<T> {
  public QueryAllTypes() {
    super(null);
  }

  @Override
  public byte[] toBinary() {
    return new byte[] {};
  }

  @Override
  public void fromBinary(final byte[] bytes) {}

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    return getClass() == obj.getClass();
  }
}
