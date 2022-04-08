/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base.dataidx;

import java.util.concurrent.CompletableFuture;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public interface BatchDataIndexRetrieval extends DataIndexRetrieval {
  CompletableFuture<GeoWaveValue[]> getDataAsync(short adapterId, byte[] dataId);

  void flush();

  void notifyIteratorInitiated();

  void notifyIteratorExhausted();
}
