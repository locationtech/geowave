/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;

public abstract class RangeReaderParamsBuilder<T, R extends RangeReaderParamsBuilder<T, R>> extends
    BaseReaderParamsBuilder<T, R> {
  protected final Index index;
  protected short[] adapterIds = null;
  protected double[] maxResolutionSubsamplingPerDimension = null;
  protected boolean isMixedVisibility = false;
  protected boolean isClientsideRowMerging = false;
  protected Integer limit = null;
  protected Integer maxRangeDecomposition = null;

  public RangeReaderParamsBuilder(
      final Index index,
      final PersistentAdapterStore adapterStore,
      final AdapterIndexMappingStore mappingStore,
      final InternalAdapterStore internalAdapterStore) {
    super(adapterStore, mappingStore, internalAdapterStore);
    this.index = index;
  }

  @Override
  protected abstract R builder();

  public R adapterIds(final short... adapterIds) {
    this.adapterIds = adapterIds;
    return builder();
  }

  public R maxResolutionSubsamplingPerDimension(
      final double[] maxResolutionSubsamplingPerDimension) {
    this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
    return builder();
  }

  public R isMixedVisibility(final boolean isMixedVisibility) {
    this.isMixedVisibility = isMixedVisibility;
    return builder();
  }

  public R isClientsideRowMerging(final boolean isClientsideRowMerging) {
    this.isClientsideRowMerging = isClientsideRowMerging;
    return builder();
  }

  public R limit(final Integer limit) {
    this.limit = limit;
    return builder();
  }

  public R maxRangeDecomposition(final Integer maxRangeDecomposition) {
    this.maxRangeDecomposition = maxRangeDecomposition;
    return builder();
  }

  @Override
  public R additionalAuthorizations(final String... authorizations) {
    additionalAuthorizations = authorizations;
    return builder();
  }
}
