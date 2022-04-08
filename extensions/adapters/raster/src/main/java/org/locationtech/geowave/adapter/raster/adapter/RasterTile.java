/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter;

import java.awt.image.DataBuffer;
import java.nio.ByteBuffer;
import org.locationtech.geowave.adapter.raster.util.DataBufferPersistenceUtils;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RasterTile<T extends Persistable> implements Mergeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RasterTile.class);
  private DataBuffer dataBuffer;
  private T metadata;

  public RasterTile() {
    super();
  }

  public RasterTile(final DataBuffer dataBuffer, final T metadata) {
    this.dataBuffer = dataBuffer;
    this.metadata = metadata;
  }

  public DataBuffer getDataBuffer() {
    return dataBuffer;
  }

  public T getMetadata() {
    return metadata;
  }

  @Override
  public byte[] toBinary() {
    final byte[] dataBufferBinary = DataBufferPersistenceUtils.getDataBufferBinary(dataBuffer);
    byte[] metadataBytes;
    if (metadata != null) {
      metadataBytes = PersistenceUtils.toBinary(metadata);
    } else {
      metadataBytes = new byte[] {};
    }
    final ByteBuffer buf =
        ByteBuffer.allocate(
            metadataBytes.length
                + dataBufferBinary.length
                + VarintUtils.unsignedIntByteLength(metadataBytes.length));
    VarintUtils.writeUnsignedInt(metadataBytes.length, buf);
    buf.put(metadataBytes);
    buf.put(dataBufferBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    try {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final int metadataLength = VarintUtils.readUnsignedInt(buf);
      if (metadataLength > 0) {
        final byte[] metadataBytes = ByteArrayUtils.safeRead(buf, metadataLength);
        metadata = (T) PersistenceUtils.fromBinary(metadataBytes);
      }
      final byte[] dataBufferBytes = new byte[buf.remaining()];
      buf.get(dataBufferBytes);
      dataBuffer = DataBufferPersistenceUtils.getDataBuffer(dataBufferBytes);
    } catch (final Exception e) {
      LOGGER.warn("Unable to deserialize data buffer", e);
    }
  }

  public void setDataBuffer(final DataBuffer dataBuffer) {
    this.dataBuffer = dataBuffer;
  }

  public void setMetadata(final T metadata) {
    this.metadata = metadata;
  }

  @Override
  public void merge(final Mergeable merge) {
    // This will get wrapped as a MergeableRasterTile by the combiner to
    // support merging
  }
}
