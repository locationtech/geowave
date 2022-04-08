/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.cql;

import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.InternalGeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.ExplicitCQLQuery;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.query.filter.FilterList;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.Expression;

public class CQLQueryFilterTest {
  SimpleFeatureType type;

  Object[] defaults;

  GeometryFactory factory = new GeometryFactory();

  @Before
  public void setup() throws SchemaException, CQLException {
    type =
        DataUtilities.createType(
            "geostuff",
            "geom:Geometry:srid=4326,pop:java.lang.Long,pid:String");

    final List<AttributeDescriptor> descriptors = type.getAttributeDescriptors();
    defaults = new Object[descriptors.size()];
    int p = 0;
    for (final AttributeDescriptor descriptor : descriptors) {
      defaults[p++] = descriptor.getDefaultValue();
    }
  }

  @Test
  public void test() {
    final FilterFactoryImpl factory = new FilterFactoryImpl();
    final Expression exp1 = factory.property("pid");
    final Expression exp2 = factory.literal("a89dhd-123-abc");
    final Filter f = factory.equal(exp1, exp2, false);
    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());

    final FeatureDataAdapter adapter = new FeatureDataAdapter(type);
    final AdapterToIndexMapping indexMapping =
        BaseDataStoreUtils.mapAdapterToIndex(adapter.asInternalAdapter((short) -1), spatialIndex);
    final InternalGeotoolsFeatureDataAdapter<SimpleFeature> internalAdapter =
        (InternalGeotoolsFeatureDataAdapter<SimpleFeature>) adapter.asInternalAdapter((short) -1);
    final ExplicitCQLQuery cqlQuery = new ExplicitCQLQuery(null, f, internalAdapter, indexMapping);

    final List<QueryFilter> filters = cqlQuery.createFilters(spatialIndex);
    final List<QueryFilter> dFilters = new ArrayList<>();
    for (final QueryFilter filter : filters) {
      dFilters.add(filter);
    }

    final FilterList dFilterList = new FilterList(dFilters);

    assertTrue(
        dFilterList.accept(
            spatialIndex.getIndexModel(),
            getEncodings(
                spatialIndex,
                internalAdapter.encode(createFeature(), indexMapping, spatialIndex)).get(0)));
  }

  private static List<IndexedAdapterPersistenceEncoding> getEncodings(
      final Index index,
      final AdapterPersistenceEncoding encoding) {
    final InsertionIds ids = encoding.getInsertionIds(index);
    final ArrayList<IndexedAdapterPersistenceEncoding> encodings = new ArrayList<>();

    for (final SinglePartitionInsertionIds partitionIds : ids.getPartitionKeys()) {
      for (final byte[] sortKey : partitionIds.getSortKeys()) {
        encodings.add(
            new IndexedAdapterPersistenceEncoding(
                encoding.getInternalAdapterId(),
                encoding.getDataId(),
                partitionIds.getPartitionKey(),
                sortKey,
                ids.getSize(),
                encoding.getCommonData(),
                encoding.getUnknownData(),
                encoding.getAdapterExtendedData()));
      }
    }
    return encodings;
  }

  private SimpleFeature createFeature() {
    final SimpleFeature instance =
        SimpleFeatureBuilder.build(type, defaults, UUID.randomUUID().toString());
    instance.setAttribute("pop", Long.valueOf(100));
    instance.setAttribute("pid", "a89dhd-123-abc");
    instance.setAttribute("geom", factory.createPoint(new Coordinate(27.25, 41.25)));
    return instance;
  }
}
