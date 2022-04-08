/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.visibility;

import java.util.List;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.VisibilityHandler;

/**
 * An implementation of visibility handler that will go through each visibility handler in a
 * provided array until it reaches a visibility that is non null.
 */
public class FallbackVisibilityHandler implements VisibilityHandler {
  private VisibilityHandler[] handlers;

  public FallbackVisibilityHandler() {}

  public FallbackVisibilityHandler(final VisibilityHandler[] handlers) {
    this.handlers = handlers;
  }

  @Override
  public <T> String getVisibility(
      final DataTypeAdapter<T> adapter,
      final T rowValue,
      final String fieldName) {
    for (VisibilityHandler handler : handlers) {
      final String visibility = handler.getVisibility(adapter, rowValue, fieldName);
      if (visibility != null) {
        return visibility;
      }
    }
    return null;
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(handlers);
  }

  @Override
  public void fromBinary(byte[] bytes) {
    final List<Persistable> handlersList = PersistenceUtils.fromBinaryAsList(bytes);
    this.handlers = handlersList.toArray(new VisibilityHandler[handlersList.size()]);
  }
}
