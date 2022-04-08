/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector;

import org.locationtech.geowave.adapter.vector.index.SimpleFeaturePrimaryIndexConfiguration;
import org.locationtech.geowave.adapter.vector.index.VectorTextIndexEntryConverter;
import org.locationtech.geowave.adapter.vector.ingest.CQLFilterOptionProvider;
import org.locationtech.geowave.adapter.vector.ingest.DataSchemaOptionProvider;
import org.locationtech.geowave.adapter.vector.ingest.FeatureSerializationOptionProvider;
import org.locationtech.geowave.adapter.vector.ingest.GeometrySimpOptionProvider;
import org.locationtech.geowave.adapter.vector.ingest.TypeNameOptionProvider;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorCountAggregation;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderAggregation;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderOptions;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderResult;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderResult.CompositeGroupResult;
import org.locationtech.geowave.adapter.vector.render.PersistableComposite;
import org.locationtech.geowave.adapter.vector.render.PersistableRenderedImage;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.index.persist.InternalPersistableRegistry;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;

public class FeatureAdapterPersistableRegistry implements
    PersistableRegistrySpi,
    InternalPersistableRegistry {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        // 500 is available
        // 501 is a legacy class (pre 2.0)
        // 502 is available
        new PersistableIdAndConstructor((short) 503, CQLFilterOptionProvider::new),
        new PersistableIdAndConstructor((short) 504, DataSchemaOptionProvider::new),
        new PersistableIdAndConstructor((short) 505, FeatureSerializationOptionProvider::new),
        new PersistableIdAndConstructor((short) 506, TypeNameOptionProvider::new),
        // 507-508 are available
        new PersistableIdAndConstructor((short) 509, DistributedRenderOptions::new),
        new PersistableIdAndConstructor((short) 510, CompositeGroupResult::new),
        new PersistableIdAndConstructor((short) 511, DistributedRenderResult::new),
        new PersistableIdAndConstructor((short) 512, PersistableComposite::new),
        new PersistableIdAndConstructor((short) 513, PersistableRenderedImage::new),
        // 514-520 is available
        new PersistableIdAndConstructor((short) 521, DistributedRenderAggregation::new),
        new PersistableIdAndConstructor((short) 522, SimpleFeatureUserDataConfigurationSet::new),
        // 523 is used by core-geotime
        // 524-526 are legacy classes (pre 2.0)
        // 527-532 are available
        // 532 is available
        new PersistableIdAndConstructor((short) 533, SimpleFeaturePrimaryIndexConfiguration::new),
        // 534 is available
        new PersistableIdAndConstructor((short) 535, VectorCountAggregation::new),
        new PersistableIdAndConstructor((short) 536, GeometrySimpOptionProvider::new),
        // 537-539 are available
        new PersistableIdAndConstructor((short) 540, VectorTextIndexEntryConverter::new),
        new PersistableIdAndConstructor((short) 541, FeatureDataAdapter::new)};
  }
}
