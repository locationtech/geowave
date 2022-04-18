/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import org.locationtech.geowave.core.index.persist.InternalPersistableRegistry;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT.DuplicateCountAggregation;
import org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT.MergeCounter;
import org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT.SumAndAveragingMergeStrategy;
import org.locationtech.geowave.test.basic.GeoWaveBasicRasterIT.SummingMergeStrategy;
import org.locationtech.geowave.test.basic.GeoWaveCustomIndexIT.TestEnumConstraints;
import org.locationtech.geowave.test.basic.GeoWaveCustomIndexIT.TestEnumIndexStrategy;
import org.locationtech.geowave.test.basic.GeoWaveVisibilityIT.TestFieldVisibilityHandler;
import org.locationtech.geowave.test.secondary.CustomSecondaryIndexIT.TestCustomConstraints;
import org.locationtech.geowave.test.secondary.CustomSecondaryIndexIT.TestCustomIndexStrategy;
import org.locationtech.geowave.test.secondary.DataIndexOnlyIT.LatLonTimeAdapter;
import org.locationtech.geowave.test.secondary.VisibilitySecondaryIndexIT.TestSecondaryIndexFieldVisibilityHandler;

public class IntegrationTestPersistableRegistry implements
    PersistableRegistrySpi,
    InternalPersistableRegistry {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        new PersistableIdAndConstructor((short) 10775, SummingMergeStrategy::new),
        new PersistableIdAndConstructor((short) 10776, SumAndAveragingMergeStrategy::new),
        new PersistableIdAndConstructor((short) 10777, MergeCounter::new),
        new PersistableIdAndConstructor((short) 10778, DuplicateCountAggregation::new),
        new PersistableIdAndConstructor((short) 10779, LatLonTimeAdapter::new),
        new PersistableIdAndConstructor((short) 10780, TestEnumConstraints::new),
        new PersistableIdAndConstructor((short) 10781, TestEnumIndexStrategy::new),
        new PersistableIdAndConstructor((short) 10782, TestFieldVisibilityHandler::new),
        new PersistableIdAndConstructor(
            (short) 10783,
            TestSecondaryIndexFieldVisibilityHandler::new),
        new PersistableIdAndConstructor((short) 10784, TestCustomConstraints::new),
        new PersistableIdAndConstructor((short) 10785, TestCustomIndexStrategy::new)};
  }
}
