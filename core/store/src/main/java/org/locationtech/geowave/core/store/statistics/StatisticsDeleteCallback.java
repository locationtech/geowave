/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

/**
 * Interface for statistics that support updates when an entry is deleted.
 */
public interface StatisticsDeleteCallback {

  /**
   * Called when an entry is deleted from the data store.
   * 
   * @param type the data type that the entry belongs to
   * @param entry the deleted entry
   * @param rows the GeoWave rows associated with the entry
   */
  public <T> void entryDeleted(DataTypeAdapter<T> type, T entry, GeoWaveRow... rows);
}
