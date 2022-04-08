/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

public interface InternalDataAdapter<T> extends DataTypeAdapter<T> {
  short getAdapterId();

  DataTypeAdapter<T> getAdapter();

  VisibilityHandler getVisibilityHandler();

  int getPositionOfOrderedField(CommonIndexModel model, String fieldName);

  String getFieldNameForPosition(CommonIndexModel model, int position);

  AdapterPersistenceEncoding encode(T entry, AdapterToIndexMapping indexMapping, final Index index);

  T decode(
      IndexedAdapterPersistenceEncoding data,
      AdapterToIndexMapping indexMapping,
      final Index index);

  boolean isCommonIndexField(AdapterToIndexMapping indexMapping, String fieldName);
}
