/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import org.locationtech.geowave.core.index.persist.Persistable;

/**
 *
 * A binning strategy is used to bin data in an aggregation query.
 *
 * @param <T> The type that will be used to aggregate on. This could be anything, but you may see
 *        things like {@code SimpleFeature}, or {@code CommonIndexedPersistenceEncoding} used
 *        mostly.
 */
public interface AggregationBinningStrategy<T> extends Persistable {

  /**
   * Given a single entry, determine appropriate bins for it.
   *
   * @param entry An entry to bin.
   * @return An array of bins to put the entry in.
   */
  String[] binEntry(T entry);
}
