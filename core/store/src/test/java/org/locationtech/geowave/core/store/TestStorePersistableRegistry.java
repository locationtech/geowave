/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store;

import org.locationtech.geowave.core.index.persist.InternalPersistableRegistry;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.core.store.adapter.AbstractDataTypeAdapterTest.TestTypeBasicDataAdapter;
import org.locationtech.geowave.core.store.adapter.AbstractDataTypeAdapterTest.TestTypeBasicDataAdapterSeparateDataID;
import org.locationtech.geowave.core.store.adapter.MockComponents.MockAbstractDataAdapter;
import org.locationtech.geowave.core.store.adapter.MockComponents.MockIndexStrategy;
import org.locationtech.geowave.core.store.adapter.MockComponents.TestDimensionField;
import org.locationtech.geowave.core.store.adapter.MockComponents.TestIndexModel;
import org.locationtech.geowave.core.store.query.BasicQueryByClassTest.ExampleDimensionOne;
import org.locationtech.geowave.core.store.query.BasicQueryByClassTest.ExampleNumericIndexStrategy;

public class TestStorePersistableRegistry implements
    PersistableRegistrySpi,
    InternalPersistableRegistry {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        new PersistableIdAndConstructor((short) 10200, MockAbstractDataAdapter::new),
        new PersistableIdAndConstructor((short) 10201, TestDimensionField::new),
        new PersistableIdAndConstructor((short) 10202, MockIndexStrategy::new),
        new PersistableIdAndConstructor((short) 10203, TestIndexModel::new),
        new PersistableIdAndConstructor((short) 10204, ExampleNumericIndexStrategy::new),
        new PersistableIdAndConstructor((short) 10205, ExampleDimensionOne::new),
        new PersistableIdAndConstructor((short) 10206, TestTypeBasicDataAdapter::new),
        new PersistableIdAndConstructor(
            (short) 10207,
            TestTypeBasicDataAdapterSeparateDataID::new)};
  }
}
