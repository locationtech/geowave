/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;

/**
 * An abstract filter that is composed of two or more other filters.
 */
public abstract class MultiFilterOperator implements Filter {

  private Filter[] children;

  public MultiFilterOperator(final Filter... children) {
    this.children = children;
  }

  public Filter[] getChildren() {
    return children;
  }

  @Override
  public void addReferencedFields(final Set<String> fields) {
    Arrays.stream(getChildren()).forEach(f -> f.addReferencedFields(fields));
  }

  @Override
  public void prepare(
      final DataTypeAdapter<?> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index) {
    Arrays.stream(children).forEach(f -> f.prepare(adapter, indexMapping, index));
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(children);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final List<Persistable> childrenList = PersistenceUtils.fromBinaryAsList(bytes);
    children = childrenList.toArray(new Filter[childrenList.size()]);
  }

}
