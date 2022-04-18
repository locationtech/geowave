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

public class GeoWaveRedisRowWithTimestampCodec extends BaseCodec {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveRedisRowWithTimestampCodec.class);
  protected static GeoWaveRedisRowWithTimestampCodec SINGLETON_WITH_VISIBILITY =
      new GeoWaveRedisRowWithTimestampCodec(true);
  protected static GeoWaveRedisRowWithTimestampCodec SINGLETON_WITHOUT_VISIBILITY =
      new GeoWaveRedisRowWithTimestampCodec(false);
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
        final GeoWaveRedisPersistedTimestampRow retVal =
            new GeoWaveRedisPersistedTimestampRow(
                (short) numDuplicates,
                dataId,
                new GeoWaveValueImpl(fieldMask, visibility, value),
                Integer.toUnsignedLong(Varint.readSignedVarInt(in)),
                Varint.readSignedVarInt(in),
                in.available() > 0 ? (short) in.readUnsignedByte() : null);
        retVal.setSortKeyPrecisionBeyondScore(sortKeyPrecisionBeyondScore);
        return retVal;
      }
    }
  };
  private final Encoder encoder = new Encoder() {
    @Override
    public ByteBuf encode(final Object in) throws IOException {
      if (in instanceof GeoWaveRedisPersistedTimestampRow) {
        final GeoWaveRedisPersistedTimestampRow row = (GeoWaveRedisPersistedTimestampRow) in;
        final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();

        try (final ByteBufOutputStream out = new ByteBufOutputStream(buf)) {
          GeoWaveRedisRowCodec.encodeRow(out, row, visibilityEnabled);
          Varint.writeSignedVarInt((int) row.getSecondsSinceEpic(), out);
          Varint.writeSignedVarInt(row.getNanoOfSecond(), out);
          if (row.getDuplicateId() != null) {
            out.writeByte(row.getDuplicateId());
          }
          out.flush();
          return out.buffer();
        }
      }
      throw new IOException("Encoder only supports GeoWaveRedisPersistedTimestampRow");
    }
  };
  private final boolean visibilityEnabled;
  private final ClassLoader classLoader;

  private GeoWaveRedisRowWithTimestampCodec(final boolean visibilityEnabled) {
    this(null, visibilityEnabled);
  }

  public GeoWaveRedisRowWithTimestampCodec(
      final ClassLoader classLoader,
      final GeoWaveRedisRowWithTimestampCodec codec) {
    this(classLoader, codec.visibilityEnabled);
  }

  private GeoWaveRedisRowWithTimestampCodec(
      final ClassLoader classLoader,
      final boolean visibilityEnabled) {
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
