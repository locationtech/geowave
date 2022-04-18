/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import java.io.IOException;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class GeoWaveMetadataCodec extends BaseCodec {
  protected static GeoWaveMetadataCodec SINGLETON_WITH_VISIBILITY = new GeoWaveMetadataCodec(true);
  protected static GeoWaveMetadataCodec SINGLETON_WITHOUT_VISIBILITY =
      new GeoWaveMetadataCodec(false);
  private final Decoder<Object> decoder = new Decoder<Object>() {
    @Override
    public Object decode(final ByteBuf buf, final State state) throws IOException {
      final byte[] primaryId = new byte[buf.readUnsignedByte()];
      final byte[] secondaryId = new byte[buf.readUnsignedByte()];
      final byte[] visibility;
      if (visibilityEnabled) {
        visibility = new byte[buf.readUnsignedByte()];
      } else {
        visibility = new byte[0];
      }
      final byte[] value = new byte[buf.readUnsignedShort()];
      buf.readBytes(primaryId);
      buf.readBytes(secondaryId);
      if (visibilityEnabled) {
        buf.readBytes(visibility);
      }
      buf.readBytes(value);
      return new GeoWaveMetadata(primaryId, secondaryId, visibility, value);
    }
  };
  private final Encoder encoder = new Encoder() {
    @Override
    public ByteBuf encode(final Object in) throws IOException {
      if (in instanceof GeoWaveMetadata) {
        return encodeMetadata((GeoWaveMetadata) in, visibilityEnabled);
      } else {
        throw new IOException("Encoder only supports GeoWave metadata");
      }
    }
  };

  protected static ByteBuf encodeMetadata(
      final GeoWaveMetadata md,
      final boolean visibilityEnabled) {
    final ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
    final byte[] safeVisibility;
    if (visibilityEnabled) {
      safeVisibility = md.getVisibility() != null ? md.getVisibility() : new byte[0];
    } else {
      safeVisibility = new byte[0];
    }
    final byte[] safeSecondaryId = md.getSecondaryId() != null ? md.getSecondaryId() : new byte[0];
    out.writeByte(md.getPrimaryId().length);
    out.writeByte(safeSecondaryId.length);
    if (visibilityEnabled) {
      out.writeByte(safeVisibility.length);
    }
    out.writeShort(md.getValue().length);
    out.writeBytes(md.getPrimaryId());
    out.writeBytes(safeSecondaryId);
    if (visibilityEnabled) {
      out.writeBytes(safeVisibility);
    }
    out.writeBytes(md.getValue());
    return out;
  }

  private final boolean visibilityEnabled;
  private final ClassLoader classLoader;

  private GeoWaveMetadataCodec(final boolean visibilityEnabled) {
    this(null, visibilityEnabled);
  }

  public GeoWaveMetadataCodec(final ClassLoader classLoader, final GeoWaveMetadataCodec codec) {
    this(classLoader, codec.visibilityEnabled);
  }

  private GeoWaveMetadataCodec(final ClassLoader classLoader, final boolean visibilityEnabled) {
    this.classLoader = classLoader;
    this.visibilityEnabled = visibilityEnabled;
  }

  @Override
  public ClassLoader getClassLoader() {
    if (classLoader != null) {
      return classLoader;
    }
    return super.getClassLoader();
  }

  @Override
  public Decoder<Object> getValueDecoder() {
    return decoder;
  }

  @Override
  public Encoder getValueEncoder() {
    return encoder;
  }
}
