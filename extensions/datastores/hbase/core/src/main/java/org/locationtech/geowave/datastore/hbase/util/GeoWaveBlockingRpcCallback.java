/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcCallback;

public class GeoWaveBlockingRpcCallback<R> implements RpcCallback<R> {
  private R result;
  private boolean resultSet = false;

  /**
   * Called on completion of the RPC call with the response object, or {@code null} in the case of
   * an error.
   *
   * @param parameter the response object or {@code null} if an error occurred
   */
  @Override
  public void run(final R parameter) {
    synchronized (this) {
      result = parameter;
      resultSet = true;
      notifyAll();
    }
  }

  /**
   * Returns the parameter passed to {@link #run(Object)} or {@code null} if a null value was
   * passed. When used asynchronously, this method will block until the {@link #run(Object)} method
   * has been called.
   *
   * @return the response object or {@code null} if no response was passed
   */
  public synchronized R get() throws IOException {
    while (!resultSet) {
      try {
        this.wait();
      } catch (final InterruptedException ie) {
        final InterruptedIOException exception = new InterruptedIOException(ie.getMessage());
        exception.initCause(ie);
        throw exception;
      }
    }
    return result;
  }
}
