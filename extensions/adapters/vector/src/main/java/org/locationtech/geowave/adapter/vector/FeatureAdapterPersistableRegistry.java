/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
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
import org.locationtech.geowave.adapter.vector.query.aggregation.CompositeVectorAggregation;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorCountAggregation;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorMaxAggregation;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorMinAggregation;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorSumAggregation;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderAggregation;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderOptions;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderResult;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderResult.CompositeGroupResult;
import org.locationtech.geowave.adapter.vector.render.PersistableComposite;
import org.locationtech.geowave.adapter.vector.render.PersistableRenderedImage;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.store.query.ExplicitCQLQuery;
import org.locationtech.geowave.core.geotime.store.query.filter.CQLQueryFilter;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors.TimeDescriptorConfiguration;
import org.locationtech.geowave.core.index.persist.InternalPersistableRegistry;
import org.locationtech.geowave.core.index.persist.PersistableList;
import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;

public class FeatureAdapterPersistableRegistry implements
    PersistableRegistrySpi,
    InternalPersistableRegistry {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return new PersistableIdAndConstructor[] {
        // 500 is available
        // 501 is a legacy class (pre 2.0)
        new PersistableIdAndConstructor((short) 502, PersistableList::new),
        new PersistableIdAndConstructor((short) 503, CQLFilterOptionProvider::new),
        new PersistableIdAndConstructor((short) 504, DataSchemaOptionProvider::new),
        new PersistableIdAndConstructor((short) 505, FeatureSerializationOptionProvider::new),
        new PersistableIdAndConstructor((short) 506, TypeNameOptionProvider::new),
        new PersistableIdAndConstructor((short) 507, ExplicitCQLQuery::new),
        new PersistableIdAndConstructor((short) 508, CQLQueryFilter::new),
        new PersistableIdAndConstructor((short) 509, DistributedRenderOptions::new),
        new PersistableIdAndConstructor((short) 510, CompositeGroupResult::new),
        new PersistableIdAndConstructor((short) 511, DistributedRenderResult::new),
        new PersistableIdAndConstructor((short) 512, PersistableComposite::new),
        new PersistableIdAndConstructor((short) 513, PersistableRenderedImage::new),
        // 514-520 is available
        new PersistableIdAndConstructor((short) 521, DistributedRenderAggregation::new),
        new PersistableIdAndConstructor((short) 522, SimpleFeatureUserDataConfigurationSet::new),
        new PersistableIdAndConstructor((short) 523, TimeDescriptorConfiguration::new),
        // 524-526 are legacy classes (pre 2.0)
        // 527-532 are available
        // 532 is available
        new PersistableIdAndConstructor((short) 533, SimpleFeaturePrimaryIndexConfiguration::new),
        new PersistableIdAndConstructor((short) 534, CompositeVectorAggregation::new),
        new PersistableIdAndConstructor((short) 535, VectorCountAggregation::new),
        new PersistableIdAndConstructor((short) 536, GeometrySimpOptionProvider::new),
        new PersistableIdAndConstructor((short) 537, VectorMinAggregation::new),
        new PersistableIdAndConstructor((short) 538, VectorMaxAggregation::new),
        new PersistableIdAndConstructor((short) 539, VectorSumAggregation::new),
        new PersistableIdAndConstructor((short) 540, VectorTextIndexEntryConverter::new),
        new PersistableIdAndConstructor((short) 541, FeatureDataAdapter::new)};
  }
}
