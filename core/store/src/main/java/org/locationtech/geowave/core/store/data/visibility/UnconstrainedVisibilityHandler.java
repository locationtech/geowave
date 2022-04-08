/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.visibility;

import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.VisibilityHandler;

/**
 * Basic implementation of a visibility handler to allow all access
 */
public class UnconstrainedVisibilityHandler implements VisibilityHandler {

  @Override
  public <T> String getVisibility(
      final DataTypeAdapter<T> adapter,
      final T rowValue,
      final String fieldName) {
    return "";
  }

  @Override
  public byte[] toBinary() {
    return new byte[0];
  }

  @Override
  public void fromBinary(byte[] bytes) {}
}
