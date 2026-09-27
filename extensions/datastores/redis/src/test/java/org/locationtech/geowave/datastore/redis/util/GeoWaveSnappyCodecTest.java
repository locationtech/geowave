/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import java.io.IOException;
import java.util.Random;
import org.junit.Test;
import org.redisson.client.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.compression.Snappy;

public class GeoWaveSnappyCodecTest {
  private final GeoWaveSnappyCodec codec = new GeoWaveSnappyCodec(ByteArrayCodec.INSTANCE);

  /** 100 bytes counting 0 to 9 over and over, as Redisson 3.15.5's SnappyCodec wrote them. */
  @Test
  public void readsWhatRedissonsSnappyCodecWrote() throws IOException {
    final byte[] expected = new byte[100];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = (byte) (i % 10);
    }
    final ByteBuf written =
        Unpooled.wrappedBuffer(
            ByteBufUtil.decodeHexDump("00000012642400010203040506070809fe0a00660a00"));
    assertArrayEquals(expected, (byte[]) codec.getValueDecoder().decode(written, null));
  }

  @Test
  public void compressesInChunksOfAtMostShortMaxValueBytes() throws IOException {
    final ByteBuf encoded = codec.getValueEncoder().encode(randomBytes(40_000));
    try {
      assertEquals(Short.MAX_VALUE, decompressedLengthOfNextChunk(encoded));
      assertEquals(40_000 - Short.MAX_VALUE, decompressedLengthOfNextChunk(encoded));
      assertFalse(encoded.isReadable());
    } finally {
      encoded.release();
    }
  }

  @Test
  public void roundTrips() throws IOException {
    for (final int length : new int[] {0, 1, Short.MAX_VALUE, Short.MAX_VALUE + 1, 100_000}) {
      final byte[] value = randomBytes(length);
      final ByteBuf encoded = codec.getValueEncoder().encode(value);
      try {
        assertArrayEquals(value, (byte[]) codec.getValueDecoder().decode(encoded, null));
      } finally {
        encoded.release();
      }
    }
  }

  private static int decompressedLengthOfNextChunk(final ByteBuf encoded) {
    final ByteBuf chunk = Unpooled.buffer();
    try {
      new Snappy().decode(encoded.readSlice(encoded.readInt()), chunk);
      return chunk.readableBytes();
    } finally {
      chunk.release();
    }
  }

  private static byte[] randomBytes(final int length) {
    final byte[] bytes = new byte[length];
    new Random(length).nextBytes(bytes);
    return bytes;
  }
}
