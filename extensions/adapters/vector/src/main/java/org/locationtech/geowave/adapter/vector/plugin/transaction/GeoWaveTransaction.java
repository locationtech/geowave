/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin.transaction;

import java.io.IOException;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

/**
 * Represent the Writer's pluggable strategy of a transaction
 */
public interface GeoWaveTransaction {

  /** Flush in memory records to store for query processing. */
  public void flush() throws IOException;

  /** @return true if transaction is empty */
  public boolean isEmpty();

  /**
   * Record a modification to the indicated fid
   *
   * @param fid the feature ID
   * @param old the original feature
   * @param updated the replacement feature; null to indicate remove
   */
  public void modify(String fid, SimpleFeature old, SimpleFeature updated) throws IOException;

  public void add(String fid, SimpleFeature f) throws IOException;

  public void remove(String fid, SimpleFeature feature) throws IOException;

  public String[] composeAuthorizations();

  public String composeVisibility();

  public StatisticsCache getDataStatistics();

  public CloseableIterator<SimpleFeature> interweaveTransaction(
      final Integer limit,
      final Filter filter,
      final CloseableIterator<SimpleFeature> it);
}
