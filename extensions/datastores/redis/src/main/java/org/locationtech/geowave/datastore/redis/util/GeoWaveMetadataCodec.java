/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

public class GeoWaveMetadataCodec extends BaseCodec {
  protected static GeoWaveMetadataCodec SINGLETON = new GeoWaveMetadataCodec();
  private final Decoder<Object> decoder =
      new Decoder<Object>() {
        @Override
        public Object decode(final ByteBuf buf, final State state) throws IOException {
          final byte[] primaryId = new byte[buf.readUnsignedByte()];
          final byte[] secondaryId = new byte[buf.readUnsignedByte()];
          final byte[] visibility = new byte[buf.readUnsignedByte()];
          final byte[] value = new byte[buf.readUnsignedShort()];
          buf.readBytes(primaryId);
          buf.readBytes(secondaryId);
          buf.readBytes(visibility);
          buf.readBytes(value);
          return new GeoWaveMetadata(primaryId, secondaryId, visibility, value);
        }
      };
  private final Encoder encoder =
      new Encoder() {
        @Override
        public ByteBuf encode(final Object in) throws IOException {
          if (in instanceof GeoWaveMetadata) {
            return encodeMetadata((GeoWaveMetadata) in);
          } else {
            throw new IOException("Encoder only supports GeoWave metadata");
          }
        }
      };

  protected static ByteBuf encodeMetadata(GeoWaveMetadata md) {
    final ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
    final byte[] safeVisibility = md.getVisibility() != null ? md.getVisibility() : new byte[0];
    final byte[] safeSecondaryId = md.getSecondaryId() != null ? md.getSecondaryId() : new byte[0];
    out.writeByte(md.getPrimaryId().length);
    out.writeByte(safeSecondaryId.length);
    out.writeByte(safeVisibility.length);
    out.writeShort(md.getValue().length);
    out.writeBytes(md.getPrimaryId());
    out.writeBytes(safeSecondaryId);
    out.writeBytes(safeVisibility);
    out.writeBytes(md.getValue());
    return out;
  }

  private GeoWaveMetadataCodec() {}

  @Override
  public Decoder<Object> getValueDecoder() {
    return decoder;
  }

  @Override
  public Encoder getValueEncoder() {
    return encoder;
  }
}
