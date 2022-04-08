/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.InsertionIds;

public class WriteResults {
  private final Map<String, InsertionIds> insertionIdsPerIndex;

  public WriteResults() {
    insertionIdsPerIndex = new HashMap<>();
  }

  public WriteResults(final String indexName, final InsertionIds insertionIds) {
    insertionIdsPerIndex = Collections.singletonMap(indexName, insertionIds);
  }

  public WriteResults(final Map<String, InsertionIds> insertionIdsPerIndex) {
    super();
    this.insertionIdsPerIndex = insertionIdsPerIndex;
  }

  public Set<String> getWrittenIndexNames() {
    return insertionIdsPerIndex.keySet();
  }

  public InsertionIds getInsertionIdsWritten(final String indexName) {
    return insertionIdsPerIndex.get(indexName);
  }

  public boolean isEmpty() {
    return insertionIdsPerIndex.isEmpty();
  }
}
