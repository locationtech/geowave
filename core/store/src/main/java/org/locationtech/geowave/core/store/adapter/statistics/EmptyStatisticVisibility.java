/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter.statistics;

import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

/**
 * Supplies not additional visibility
 *
 * @param <T>
 */
public class EmptyStatisticVisibility<T> implements EntryVisibilityHandler<T> {

  @Override
  public byte[] getVisibility(final T entry, final GeoWaveRow... kvs) {
    return new byte[0];
  }
}
