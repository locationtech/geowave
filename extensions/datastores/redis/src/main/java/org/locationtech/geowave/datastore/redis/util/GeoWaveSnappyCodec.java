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
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.compression.Snappy;

/**
 * Snappy compression around another codec, in the format Redis stores have always been written in:
 * the inner codec's bytes in chunks of at most {@value #MAX_CHUNK_LENGTH}, each compressed and
 * preceded by its compressed length. That is Redisson's SnappyCodec format. Redisson deprecated
 * SnappyCodec and Redisson 4 removes it; its replacement, SnappyCodecV2, writes a different format
 * and cannot read existing data.
 */
public class GeoWaveSnappyCodec extends BaseCodec {
  private static final int MAX_CHUNK_LENGTH = Short.MAX_VALUE;

  private final Codec innerCodec;
  private final Encoder encoder = this::compress;
  private final Decoder<Object> decoder = this::decompress;

  public GeoWaveSnappyCodec(final Codec innerCodec) {
    this.innerCodec = innerCodec;
  }

  public GeoWaveSnappyCodec(final ClassLoader classLoader, final GeoWaveSnappyCodec codec)
      throws ReflectiveOperationException {
    this(copy(classLoader, codec.innerCodec));
  }

  private ByteBuf compress(final Object value) throws IOException {
    final ByteBuf uncompressed = innerCodec.getValueEncoder().encode(value);
    final ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
    try {
      final Snappy snappy = new Snappy();
      while (uncompressed.isReadable()) {
        final ByteBuf chunk =
            uncompressed.readSlice(Math.min(MAX_CHUNK_LENGTH, uncompressed.readableBytes()));
        final int lengthIndex = out.writerIndex();
        out.writeInt(0);
        snappy.encode(chunk, out, chunk.readableBytes());
        out.setInt(lengthIndex, out.writerIndex() - lengthIndex - Integer.BYTES);
      }
      return out;
    } catch (final RuntimeException e) {
      out.release();
      throw e;
    } finally {
      uncompressed.release();
    }
  }

  private Object decompress(final ByteBuf compressed, final State state) throws IOException {
    final ByteBuf uncompressed = ByteBufAllocator.DEFAULT.buffer();
    try {
      final Snappy snappy = new Snappy();
      while (compressed.isReadable()) {
        snappy.decode(compressed.readSlice(compressed.readInt()), uncompressed);
        snappy.reset();
      }
      return innerCodec.getValueDecoder().decode(uncompressed, state);
    } finally {
      uncompressed.release();
    }
  }

  @Override
  public Decoder<Object> getValueDecoder() {
    return decoder;
  }

  @Override
  public Encoder getValueEncoder() {
    return encoder;
  }

  @Override
  public ClassLoader getClassLoader() {
    return innerCodec.getClassLoader();
  }
}
