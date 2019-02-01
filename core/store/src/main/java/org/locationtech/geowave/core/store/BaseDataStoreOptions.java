/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store;

import com.beust.jcommander.Parameter;

public class BaseDataStoreOptions implements DataStoreOptions {
  @Parameter(names = "--persistDataStatistics", hidden = true, arity = 1)
  protected boolean persistDataStatistics = true;

  @Parameter(names = "--enableBlockCache", hidden = true, arity = 1)
  protected boolean enableBlockCache = true;

  @Parameter(names = "--enableServerSideLibrary", arity = 1)
  protected boolean enableServerSideLibrary = true;

  @Parameter(names = "--enableSecondaryIndexing")
  protected boolean enableSecondaryIndexing = false;

  @Parameter(names = "--enableVisibility", arity = 1)
  protected Boolean configuredEnableVisibility = null;

  @Parameter(names = "--dataIndexBatchSize")
  protected int configuredDataIndexBatchSize = Integer.MIN_VALUE;

  @Parameter(names = "--maxRangeDecomposition", arity = 1)
  protected int configuredMaxRangeDecomposition = Integer.MIN_VALUE;

  @Parameter(names = "--aggregationMaxRangeDecomposition", arity = 1)
  protected int configuredAggregationMaxRangeDecomposition = Integer.MIN_VALUE;

  @Override
  public boolean isPersistDataStatistics() {
    return persistDataStatistics;
  }

  public void setPersistDataStatistics(final boolean persistDataStatistics) {
    this.persistDataStatistics = persistDataStatistics;
  }

  @Override
  public boolean isSecondaryIndexing() {
    return enableSecondaryIndexing;
  }

  @Override
  public void setSecondaryIndexing(final boolean enableSecondaryIndexing) {
    this.enableSecondaryIndexing = enableSecondaryIndexing;
  }

  @Override
  public boolean isEnableBlockCache() {
    return enableBlockCache;
  }

  public void setEnableBlockCache(final boolean enableBlockCache) {
    this.enableBlockCache = enableBlockCache;
  }

  @Override
  public boolean isServerSideLibraryEnabled() {
    return enableServerSideLibrary && !enableSecondaryIndexing;
  }

  public void setServerSideLibraryEnabled(final boolean enableServerSideLibrary) {
    this.enableServerSideLibrary = enableServerSideLibrary;
  }

  @Override
  public int getMaxRangeDecomposition() {
    return configuredMaxRangeDecomposition == Integer.MIN_VALUE ? defaultMaxRangeDecomposition()
        : configuredMaxRangeDecomposition;
  }

  protected int defaultMaxRangeDecomposition() {
    return 2000;
  }

  protected boolean defaultEnableVisibility() {
    return true;
  }

  public void setMaxRangeDecomposition(final int maxRangeDecomposition) {
    configuredMaxRangeDecomposition = maxRangeDecomposition;
  }

  @Override
  public int getAggregationMaxRangeDecomposition() {
    return configuredAggregationMaxRangeDecomposition == Integer.MIN_VALUE
        ? defaultAggregationMaxRangeDecomposition()
        : configuredAggregationMaxRangeDecomposition;
  }

  @Override
  public int getDataIndexBatchSize() {
    return isSecondaryIndexing()
        ? (configuredDataIndexBatchSize == Integer.MIN_VALUE ? defaultDataIndexBatchSize()
            : configuredDataIndexBatchSize)
        : Integer.MIN_VALUE;
  }

  protected int defaultDataIndexBatchSize() {
    return 2000;
  }

  protected int defaultAggregationMaxRangeDecomposition() {
    return 10;
  }

  public void setAggregationMaxRangeDecomposition(final int aggregationMaxRangeDecomposition) {
    configuredAggregationMaxRangeDecomposition = aggregationMaxRangeDecomposition;
  }

  @Override
  public boolean isVisibilityEnabled() {
    return configuredEnableVisibility == null ? defaultEnableVisibility()
        : configuredEnableVisibility;
  }

  public void setEnableVisibility(final boolean configuredEnableVisibility) {
    this.configuredEnableVisibility = configuredEnableVisibility;
  }
}
