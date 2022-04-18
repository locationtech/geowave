/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

public class RocksDBRowIterator extends AbstractRocksDBIterator<GeoWaveRow> {
  private final short adapterId;
  private final byte[] partition;
  private final boolean containsTimestamp;
  private final boolean visibilityEnabled;

  public RocksDBRowIterator(
      final ReadOptions options,
      final RocksIterator it,
      final short adapterId,
      final byte[] partition,
      final boolean containsTimestamp,
      final boolean visiblityEnabled) {
    super(options, it);
    this.adapterId = adapterId;
    this.partition = partition;
    this.containsTimestamp = containsTimestamp;
    visibilityEnabled = visiblityEnabled;
  }

  @Override
  protected GeoWaveRow readRow(final byte[] key, final byte[] value) {
    return new RocksDBRow(adapterId, partition, key, value, containsTimestamp, visibilityEnabled);
  }
}
