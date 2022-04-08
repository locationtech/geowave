/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import org.apache.commons.lang.ArrayUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;

/**
 * Abstract class for performing optimal aggregations on adapter fields.
 *
 * @param <R> the aggregation return type
 * @param <T> the adapter type
 */
public abstract class OptimalFieldAggregation<R, T> implements
    AdapterAndIndexBasedAggregation<FieldNameParam, R, T> {
  protected FieldNameParam fieldNameParam;

  public OptimalFieldAggregation() {}

  public OptimalFieldAggregation(final FieldNameParam fieldNameParam) {
    this.fieldNameParam = fieldNameParam;
  }

  @Override
  public FieldNameParam getParameters() {
    return fieldNameParam;
  }

  @Override
  public void setParameters(final FieldNameParam parameters) {
    fieldNameParam = parameters;
  }

  @Override
  public Aggregation<FieldNameParam, R, ?> createAggregation(
      final DataTypeAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    if (fieldNameParam == null
        || indexMapping.getIndexFieldMappers().stream().anyMatch(
            m -> ArrayUtils.contains(m.getAdapterFields(), fieldNameParam.getFieldName()))) {
      return createCommonIndexAggregation();
    }
    return createAggregation();
  }

  protected abstract Aggregation<FieldNameParam, R, CommonIndexedPersistenceEncoding> createCommonIndexAggregation();

  protected abstract Aggregation<FieldNameParam, R, T> createAggregation();
}
