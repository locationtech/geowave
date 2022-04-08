/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.callback;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;

/**
 * This interface provides a callback mechanism when deleting a collection of entries.
 *
 * @param <T> A generic type for entries
 * @param <R> A generic type for rows
 */
public interface DeleteCallback<T, R extends GeoWaveRow> {
  /**
   * This will be called after an entry is successfully deleted with the row IDs that were used
   *
   * @param entry the entry that was deleted
   * @param rows the raw rows that were deleted
   */
  public void entryDeleted(final T entry, final R... rows);
}
