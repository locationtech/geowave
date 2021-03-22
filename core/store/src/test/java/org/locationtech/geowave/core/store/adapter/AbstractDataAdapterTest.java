/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.MockComponents.MockAbstractDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.index.CustomNameIndex;

public class AbstractDataAdapterTest {

  @Test
  // *************************************************************************
  //
  // Test encode(..) and decode(..) methods of AbstractDataAdapter via
  // instantiation of MockAbstractDataAdapterTest.
  //
  // *************************************************************************
  public void testAbstractDataAdapterEncodeDecode() {

    final MockComponents.MockAbstractDataAdapter mockAbstractDataAdapter =
        new MockComponents.MockAbstractDataAdapter();
    final MockComponents.TestIndexModel testIndexModel = new MockComponents.TestIndexModel();
    final Integer beforeValue = 123456;
    final InternalDataAdapter<Integer> adapter =
        mockAbstractDataAdapter.asInternalAdapter((short) -1);
    final Index index = new CustomNameIndex(null, testIndexModel, "idx");
    final AdapterToIndexMapping indexMapping = BaseDataStoreUtils.mapAdapterToIndex(adapter, index);
    final AdapterPersistenceEncoding testEncoding =
        adapter.encode(beforeValue, indexMapping, index);
    final Integer afterValue =
        adapter.decode(
            new IndexedAdapterPersistenceEncoding(
                testEncoding.getInternalAdapterId(),
                testEncoding.getDataId(),
                null,
                null,
                1,
                testEncoding.getCommonData(),
                new MultiFieldPersistentDataset<byte[]>(),
                testEncoding.getAdapterExtendedData()),
            indexMapping,
            index);

    Assert.assertEquals("EncodeDecode_test", beforeValue, afterValue);
  }

  @Test
  public void testAbstractDataAdapterPersistance() {
    final MockAbstractDataAdapter mockAbstractDataAdapter = new MockAbstractDataAdapter();

    final MockAbstractDataAdapter obj =
        (MockAbstractDataAdapter) PersistenceUtils.fromBinary(
            PersistenceUtils.toBinary(mockAbstractDataAdapter));

    // TODO is there another test?
    Assert.assertNotNull(obj);
  }
}
