/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store;

import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterImpl;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class InternalGeotoolsDataAdapterWrapper<T extends SimpleFeature> extends
    InternalDataAdapterImpl<T> implements
    InternalGeotoolsFeatureDataAdapter<T> {

  public InternalGeotoolsDataAdapterWrapper() {
    super();
  }

  public InternalGeotoolsDataAdapterWrapper(
      final GeotoolsFeatureDataAdapter<T> adapter,
      final short adapterId) {
    super(adapter, adapterId);
  }

  public InternalGeotoolsDataAdapterWrapper(
      final GeotoolsFeatureDataAdapter<T> adapter,
      final short adapterId,
      final VisibilityHandler visibilityHandler) {
    super(adapter, adapterId, visibilityHandler);
  }

  @Override
  public SimpleFeatureType getFeatureType() {
    return ((GeotoolsFeatureDataAdapter<T>) adapter).getFeatureType();
  }

  @Override
  public TimeDescriptors getTimeDescriptors() {
    return ((GeotoolsFeatureDataAdapter<T>) adapter).getTimeDescriptors();
  }

  @Override
  public boolean hasTemporalConstraints() {
    return ((GeotoolsFeatureDataAdapter<T>) adapter).hasTemporalConstraints();
  }

  @Override
  public void setNamespace(final String namespaceURI) {
    ((GeotoolsFeatureDataAdapter<T>) adapter).setNamespace(namespaceURI);
  }

}
