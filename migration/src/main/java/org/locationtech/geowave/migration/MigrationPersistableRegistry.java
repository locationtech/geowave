/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.migration;

import org.locationtech.geowave.core.index.persist.InternalPersistableRegistry;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;
import org.locationtech.geowave.migration.legacy.adapter.LegacyInternalDataAdapterWrapper;
import org.locationtech.geowave.migration.legacy.adapter.vector.LegacyFeatureDataAdapter;
import org.locationtech.geowave.migration.legacy.adapter.vector.LegacyStatsConfigurationCollection;
import org.locationtech.geowave.migration.legacy.adapter.vector.LegacyStatsConfigurationCollection.LegacySimpleFeatureStatsConfigurationCollection;
import org.locationtech.geowave.migration.legacy.adapter.vector.LegacyVisibilityConfiguration;
import org.locationtech.geowave.migration.legacy.core.geotime.LegacyCustomCRSSpatialField;
import org.locationtech.geowave.migration.legacy.core.geotime.LegacyLatitudeField;
import org.locationtech.geowave.migration.legacy.core.geotime.LegacyLongitudeField;
import org.locationtech.geowave.migration.legacy.core.store.LegacyAdapterToIndexMapping;

public class MigrationPersistableRegistry implements
    PersistableRegistrySpi,
    InternalPersistableRegistry {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        new PersistableIdAndConstructor((short) 200, LegacyAdapterToIndexMapping::new),
        new PersistableIdAndConstructor((short) 262, LegacyInternalDataAdapterWrapper::new),
        new PersistableIdAndConstructor((short) 304, LegacyLatitudeField::new),
        new PersistableIdAndConstructor((short) 305, LegacyLongitudeField::new),
        new PersistableIdAndConstructor((short) 313, LegacyCustomCRSSpatialField::new),
        new PersistableIdAndConstructor((short) 501, LegacyFeatureDataAdapter::new),
        new PersistableIdAndConstructor((short) 524, LegacyVisibilityConfiguration::new),
        new PersistableIdAndConstructor(
            (short) 525,
            LegacySimpleFeatureStatsConfigurationCollection::new),
        new PersistableIdAndConstructor((short) 526, LegacyStatsConfigurationCollection::new),//
    };
  }
}
