/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.hdfs.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferBackedInputStream extends InputStream {
  private final ByteBuffer buf;

  public ByteBufferBackedInputStream(final ByteBuffer buf) {
    this.buf = buf;
  }

  @Override
  public int read() throws IOException {
    if (!buf.hasRemaining()) {
      return -1;
    }
    return buf.get() & 0xFF;
  }

  @Override
  public int read(final byte[] bytes, final int off, int len) throws IOException {
    if (!buf.hasRemaining()) {
      return -1;
    }

    len = Math.min(len, buf.remaining());
    buf.get(bytes, off, len);
    return len;
  }

  @Override
  public int available() throws IOException {
    return buf.remaining();
  }

  @Override
  public int read(final byte[] bytes) throws IOException {
    if (!buf.hasRemaining()) {
      return -1;
    }

    final int len = Math.min(bytes.length, buf.remaining());
    buf.get(bytes, 0, len);
    return len;
  }

  @Override
  public synchronized void reset() throws IOException {
    buf.reset();
  }

  @Override
  public long skip(final long len) throws IOException {
    buf.get(new byte[(int) len]);
    return len;
  }
}
