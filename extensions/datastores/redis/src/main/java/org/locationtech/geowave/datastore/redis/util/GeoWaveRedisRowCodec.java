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
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.clearspring.analytics.util.Varint;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

public class GeoWaveRedisRowCodec extends BaseCodec {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveRedisRowCodec.class);
  protected static GeoWaveRedisRowCodec SINGLETON_WITH_VISIBILITY = new GeoWaveRedisRowCodec(true);
  protected static GeoWaveRedisRowCodec SINGLETON_WITHOUT_VISIBILITY =
      new GeoWaveRedisRowCodec(true);
  private final Decoder<Object> decoder = new Decoder<Object>() {
    @Override
    public Object decode(final ByteBuf buf, final State state) throws IOException {
      try (final ByteBufInputStream in = new ByteBufInputStream(buf)) {
        final byte[] dataId = new byte[in.readUnsignedByte()];
        final byte[] fieldMask = new byte[in.readUnsignedByte()];
        final byte[] visibility;
        if (visibilityEnabled) {
          visibility = new byte[in.readUnsignedByte()];
        } else {
          visibility = new byte[0];
        }
        final byte[] sortKeyPrecisionBeyondScore = new byte[Varint.readUnsignedVarInt(in)];
        final byte[] value = new byte[Varint.readUnsignedVarInt(in)];
        final int numDuplicates = in.readUnsignedByte();
        if ((dataId.length > 0) && (in.read(dataId) != dataId.length)) {
          LOGGER.warn("unable to read data ID");
        }
        if ((fieldMask.length > 0) && (in.read(fieldMask) != fieldMask.length)) {
          LOGGER.warn("unable to read fieldMask");
        }
        if (visibilityEnabled
            && (visibility.length > 0)
            && (in.read(visibility) != visibility.length)) {
          LOGGER.warn("unable to read visibility");
        }
        if ((sortKeyPrecisionBeyondScore.length > 0)
            && (in.read(sortKeyPrecisionBeyondScore) != sortKeyPrecisionBeyondScore.length)) {
          LOGGER.warn("unable to read sortKey");
        }
        if ((value.length > 0) && (in.read(value) != value.length)) {
          LOGGER.warn("unable to read value");
        }
        final GeoWaveRedisPersistedRow retVal =
            new GeoWaveRedisPersistedRow(
                (short) numDuplicates,
                dataId,
                new GeoWaveValueImpl(fieldMask, visibility, value),
                in.available() > 0 ? (short) in.readUnsignedByte() : null);
        retVal.setSortKeyPrecisionBeyondScore(sortKeyPrecisionBeyondScore);
        return retVal;
      }
    }
  };
  private final Encoder encoder = new Encoder() {
    @Override
    public ByteBuf encode(final Object in) throws IOException {
      if (in instanceof GeoWaveRedisPersistedRow) {
        final GeoWaveRedisPersistedRow row = (GeoWaveRedisPersistedRow) in;
        final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();

        try (final ByteBufOutputStream out = new ByteBufOutputStream(buf)) {
          encodeRow(out, row, visibilityEnabled);
          if (row.getDuplicateId() != null) {
            out.writeByte(row.getDuplicateId());
          }
          out.flush();
          return out.buffer();
        }
      }
      throw new IOException("Encoder only supports GeoWaveRedisRow");
    }
  };

  protected static void encodeRow(
      final ByteBufOutputStream out,
      final GeoWaveRedisPersistedRow row,
      final boolean visibilityEnabled) throws IOException {
    out.writeByte(row.getDataId().length);
    out.writeByte(row.getFieldMask().length);
    if (visibilityEnabled) {
      out.writeByte(row.getVisibility().length);
    }
    Varint.writeUnsignedVarInt(row.getSortKeyPrecisionBeyondScore().length, out);
    Varint.writeUnsignedVarInt(row.getValue().length, out);
    out.writeByte(row.getNumDuplicates());
    out.write(row.getDataId());
    out.write(row.getFieldMask());
    if (visibilityEnabled) {
      out.write(row.getVisibility());
    }
    out.write(row.getSortKeyPrecisionBeyondScore());
    out.write(row.getValue());
  }

  private final boolean visibilityEnabled;
  private final ClassLoader classLoader;

  private GeoWaveRedisRowCodec(final boolean visibilityEnabled) {
    this(null, visibilityEnabled);
  }

  public GeoWaveRedisRowCodec(final ClassLoader classLoader, final GeoWaveRedisRowCodec codec) {
    this(classLoader, codec.visibilityEnabled);
  }

  private GeoWaveRedisRowCodec(final ClassLoader classLoader, final boolean visibilityEnabled) {
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
