/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.util;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

public class RocksDBMetadataIterator extends AbstractRocksDBIterator<GeoWaveMetadata> {
  private final boolean containsTimestamp;
  private final boolean visibilityEnabled;

  public RocksDBMetadataIterator(
      final RocksIterator it,
      final boolean containsTimestamp,
      final boolean visibilityEnabled) {
    this(null, it, containsTimestamp, visibilityEnabled);
  }

  public RocksDBMetadataIterator(
      final ReadOptions options,
      final RocksIterator it,
      final boolean containsTimestamp,
      final boolean visibilityEnabled) {
    super(options, it);
    this.it = it;
    this.containsTimestamp = containsTimestamp;
    this.visibilityEnabled = visibilityEnabled;
  }

  @Override
  protected GeoWaveMetadata readRow(final byte[] key, final byte[] value) {
    final ByteBuffer buf = ByteBuffer.wrap(key);
    final byte[] primaryId = new byte[Byte.toUnsignedInt(key[key.length - 1])];
    final byte[] visibility;

    if (visibilityEnabled) {
      visibility = new byte[Byte.toUnsignedInt(key[key.length - 2])];
    } else {
      visibility = new byte[0];
    }
    int secondaryIdLength = key.length - primaryId.length - visibility.length - 1;
    if (containsTimestamp) {
      secondaryIdLength -= 8;
    }
    if (visibilityEnabled) {
      secondaryIdLength--;
    }
    final byte[] secondaryId = new byte[secondaryIdLength];
    buf.get(primaryId);
    buf.get(secondaryId);
    if (containsTimestamp) {
      // just skip 8 bytes - we don't care to parse out the timestamp but
      // its there for key uniqueness and to maintain expected sort order
      buf.position(buf.position() + 8);
    }
    if (visibilityEnabled) {
      buf.get(visibility);
    }

    return new RocksDBGeoWaveMetadata(primaryId, secondaryId, visibility, value, key);
  }
}
