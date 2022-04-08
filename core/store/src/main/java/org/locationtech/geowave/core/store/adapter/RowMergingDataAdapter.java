/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

public interface RowMergingDataAdapter<T, M extends Mergeable> extends DataTypeAdapter<T> {
  default RowTransform<M> getTransform() {
    return new SimpleRowTransform(mergeableClassId());
  }

  default Short mergeableClassId() {
    return null;
  }

  default Map<String, String> getOptions(
      final short internalAdapterId,
      final Map<String, String> existingOptions) {
    return Collections.EMPTY_MAP;
  }

  static interface RowTransform<M extends Mergeable> extends Persistable {
    void initOptions(final Map<String, String> options) throws IOException;

    M getRowAsMergeableObject(
        final short internalAdapterId,
        final ByteArray fieldId,
        final byte[] rowValueBinary);

    byte[] getBinaryFromMergedObject(final M rowObject);

    String getTransformName();

    int getBaseTransformPriority();
  }
}
