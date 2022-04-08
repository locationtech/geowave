/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime;

import org.locationtech.geowave.core.geotime.store.data.PersistenceEncodingTest.GeoObjDataAdapter;
import org.locationtech.geowave.core.geotime.util.GeometryUtilsTest.ExampleNumericIndexStrategy;
import org.locationtech.geowave.core.index.persist.InternalPersistableRegistry;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;

public class TestGeoTimePersistableRegistry implements
    PersistableRegistrySpi,
    InternalPersistableRegistry {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        new PersistableIdAndConstructor((short) 10300, ExampleNumericIndexStrategy::new),
        new PersistableIdAndConstructor((short) 10301, GeoObjDataAdapter::new),};
  }
}
