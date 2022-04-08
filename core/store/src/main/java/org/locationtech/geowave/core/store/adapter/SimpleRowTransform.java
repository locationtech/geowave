/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.PersistableFactory;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;

public class SimpleRowTransform<M extends Mergeable> implements RowTransform<M> {
  private Short classId;

  public SimpleRowTransform() {
    this(null);
  }

  public SimpleRowTransform(final Short classId) {
    this.classId = classId;
  }

  @Override
  public byte[] toBinary() {
    if (classId != null) {
      return ByteBuffer.allocate(2).putShort(classId).array();
    }
    return new byte[0];
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    if (bytes.length > 1) {
      classId = ByteBuffer.wrap(bytes).getShort();
    }
  }

  @Override
  public void initOptions(final Map<String, String> options) throws IOException {}

  @Override
  public M getRowAsMergeableObject(
      final short internalAdapterId,
      final ByteArray fieldId,
      final byte[] rowValueBinary) {
    // if class ID is non-null then we can short-circuit reading it from the binary
    if (classId != null) {
      final M newInstance = (M) PersistableFactory.getInstance().newInstance(classId);
      newInstance.fromBinary(rowValueBinary);
      return newInstance;
    }
    return (M) PersistenceUtils.fromBinary(rowValueBinary);
  }

  @Override
  public byte[] getBinaryFromMergedObject(final M rowObject) {
    // if class ID is non-null then we can short-circuit writing it too
    if (classId != null) {
      if (rowObject != null) {
        return rowObject.toBinary();
      }
      return new byte[0];
    }
    return PersistenceUtils.toBinary(rowObject);
  }

  @Override
  public String getTransformName() {
    return "default";
  }

  @Override
  public int getBaseTransformPriority() {
    return 0;
  }
}
