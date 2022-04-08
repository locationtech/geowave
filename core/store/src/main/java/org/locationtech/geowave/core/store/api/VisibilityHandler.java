/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.persist.Persistable;

/**
 * This interface serves to provide visibility information for a given field of an adapter entry.
 */
public interface VisibilityHandler extends Persistable {
  /**
   * Determine visibility of the field.
   *
   * @param adapter the adapter for the entry
   * @param entry the entry
   * @param fieldName the field to determine visibility for
   * @return The visibility for the field
   */
  public <T> String getVisibility(DataTypeAdapter<T> adapter, T entry, String fieldName);
}
