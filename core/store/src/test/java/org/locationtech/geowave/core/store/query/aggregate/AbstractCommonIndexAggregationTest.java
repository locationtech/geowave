/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import java.util.List;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.adapter.MockComponents;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.SingleFieldPersistentDataset;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import com.google.common.collect.Lists;

public abstract class AbstractCommonIndexAggregationTest<P extends Persistable, R> extends
    AbstractAggregationTest<P, R, CommonIndexedPersistenceEncoding> {

  public static List<CommonIndexedPersistenceEncoding> generateObjects(final int count) {
    List<CommonIndexedPersistenceEncoding> objects = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      String dataId = "entry" + i;
      PersistentDataset<CommonIndexValue> commonData = new MultiFieldPersistentDataset<>();
      commonData.addValue("value", new MockComponents.TestIndexFieldType(i));
      objects.add(
          new CommonIndexedPersistenceEncoding(
              (short) 0,
              dataId.getBytes(),
              new byte[0],
              new byte[0],
              0,
              commonData,
              new SingleFieldPersistentDataset<byte[]>()));
    }
    return objects;
  }

}
