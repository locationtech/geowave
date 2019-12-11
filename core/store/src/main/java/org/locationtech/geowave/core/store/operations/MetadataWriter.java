/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

/**
 * Provides an interface for persisting metadata.
 *
 * Writes may only be performed as long as the instance is not closed.
 */
public interface MetadataWriter extends AutoCloseable {
  /**
   * Write metadata to the table.
   *
   * Preconditions:
   * <ul> <li>The writer is not closed</li> </ul>
   *
   * @param metadata The metadata.
   */
  void write(GeoWaveMetadata metadata);

  /**
   * Flush the writer, committing all pending writes. Note that the writes may already be committed
   * - this method just establishes that they *must* be committed after the method returns.
   *
   * Preconditions:
   * <ul> <li>The writer is not closed</li> </ul>
   */
  void flush();
}
