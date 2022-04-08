/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter;

import java.util.List;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.threeten.extra.Interval;

/**
 * Maps an adapter temporal field or fields to an `Interval` index field.
 *
 * @param <N> the adapter field type
 */
public abstract class TemporalIntervalFieldMapper<N> extends IndexFieldMapper<N, Interval> {

  @Override
  public Class<Interval> indexFieldType() {
    return Interval.class;
  }

  @Override
  public void transformFieldDescriptors(final FieldDescriptor<?>[] inputFieldDescriptors) {}

  @Override
  protected void initFromOptions(
      List<FieldDescriptor<N>> inputFieldDescriptors,
      IndexFieldOptions options) {}

}
