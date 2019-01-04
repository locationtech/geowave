/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

public class GeoWaveMetadataWithTimestampCodec extends BaseCodec {
  protected static GeoWaveMetadataWithTimestampCodec SINGLETON =
      new GeoWaveMetadataWithTimestampCodec();
  private final Decoder<Object> decoder = new Decoder<Object>() {
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
      return new GeoWaveTimestampMetadata(
          primaryId,
          secondaryId,
          visibility,
          value,
          buf.readLong());
    }
  };
  private final Encoder encoder = new Encoder() {
    @Override
    public ByteBuf encode(final Object in) throws IOException {
      if (in instanceof GeoWaveTimestampMetadata) {
        final GeoWaveTimestampMetadata md = (GeoWaveTimestampMetadata) in;
        final ByteBuf out = GeoWaveMetadataCodec.encodeMetadata(md);
        out.writeLong(md.getMillisFromEpoch());
        return out;
      } else {
        throw new IOException("Encoder only supports GeoWave timestamp metadata");
      }
    }
  };

  private GeoWaveMetadataWithTimestampCodec() {}

  @Override
  public Decoder<Object> getValueDecoder() {
    return decoder;
  }

  @Override
  public Encoder getValueEncoder() {
    return encoder;
  }
}
