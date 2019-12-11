/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

/**
 * TProvides an interface for deleting GeoWave metadata. A {@link MetadataQuery} is used to specify the metadata to be deleted.
 *
 * Delete queries may only be performed if the deleter is not closed.
 */
public interface MetadataDeleter extends AutoCloseable {
  /**
   * Delete metadata from the DB.
   *
   * Preconditions:
   * <ul> <li>The deleter is not closed</li> </ul>
   *
   * @param query The query that specifies the metadata to be deleted.
   * @return {@code true} if an object matching the query was found and successfully deleted, {@code false} otherwise.
   */
  boolean delete(MetadataQuery query);

  /**
   * Flush the deleter, committing all pending changes. Note that the changes may already be
   * committed - this method just establishes that they *must* be committed after the method
   * returns.
   *
   * Preconditions: <ul> <li>The deleter is not closed</li> </ul>
   */
  void flush();
}
