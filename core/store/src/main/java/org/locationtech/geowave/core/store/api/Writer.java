/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.io.Closeable;

public interface Writer<T> extends Closeable {
  /**
   * Writes an entry using default visibilities set elsewhere.
   *
   * @param entry the entry to write
   * @return the Insertion IDs representing where this entry was written
   */
  WriteResults write(final T entry);

  /**
   * Writes an entry using the provided visibility handler.
   *
   * @param entry the entry to write
   * @param visibilityHandler the handler for determining field visibility
   * @return the Insertion IDs representing where this entry was written
   */
  WriteResults write(final T entry, final VisibilityHandler visibilityHandler);

  /**
   * Get the indices that are being written to.
   *
   * @return the indices that are being written to
   */
  Index[] getIndices();

  /**
   * Flush the underlying row writer to ensure entries queued for write are fully written. This is
   * particularly useful for streaming data as an intermittent mechanism to ensure periodic updates
   * are being stored.
   */
  void flush();

  /** Flush all entries enqueued and close all resources for this writer. */
  @Override
  void close();
}
