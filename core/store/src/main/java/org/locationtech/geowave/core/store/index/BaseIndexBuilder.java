/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index;

import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.IndexPluginOptions.PartitionStrategy;

public abstract class BaseIndexBuilder<T extends IndexBuilder> implements IndexBuilder {
  private final IndexPluginOptions options;

  public BaseIndexBuilder() {
    this(new IndexPluginOptions());
  }

  private BaseIndexBuilder(final IndexPluginOptions options) {
    this.options = options;
  }

  public T setNumPartitions(final int numPartitions) {
    options.getBasicIndexOptions().setNumPartitions(numPartitions);
    return (T) this;
  }

  public T setPartitionStrategy(final PartitionStrategy partitionStrategy) {
    options.getBasicIndexOptions().setPartitionStrategy(partitionStrategy);
    return (T) this;
  }

  public T setName(final String indexName) {
    options.setName(indexName);
    return (T) this;
  }

  public Index createIndex(final Index dimensionalityIndex) {
    return IndexPluginOptions.wrapIndexWithOptions(dimensionalityIndex, options);
  }
}
