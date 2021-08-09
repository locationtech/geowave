/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.query.gwql.parse.GWQLParser;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.opengis.feature.simple.SimpleFeatureType;

public abstract class AbstractGWQLTest {

  protected DataStore createDataStore() {
    return createDataStore(
        "type",
        "geometry:Geometry:srid=4326,pop:java.lang.Long,start:Date,end:Date,pid:String");
  }

  protected DataStore createDataStore(final String typeName, final String typeDefinition) {
    final StoreFactoryFamilySpi storeFamily = new MemoryStoreFactoryFamily();
    final MemoryRequiredOptions opts = new MemoryRequiredOptions();
    opts.setGeoWaveNamespace("test_" + getClass().getName());
    final DataStore dataStore = storeFamily.getDataStoreFactory().createStore(opts);
    final Index spatialIndex =
        SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
    final FeatureDataAdapter adapter = createDataAdapter(typeName, typeDefinition);
    dataStore.addType(adapter, spatialIndex);
    return dataStore;
  }

  protected void assertInvalidStatement(
      final DataStore dataStore,
      final String statement,
      final String expectedMessage) {
    try {
      GWQLParser.parseStatement(dataStore, statement);
      fail();
    } catch (GWQLParseException e) {
      // expected
      assertTrue(
          e.getMessage() + " does not contain " + expectedMessage,
          e.getMessage().contains(expectedMessage));
    }
  }

  private FeatureDataAdapter createDataAdapter(final String typeName, final String typeDefinition) {
    try {
      final SimpleFeatureType featureType = DataUtilities.createType(typeName, typeDefinition);
      return new FeatureDataAdapter(featureType);
    } catch (SchemaException e) {
      fail();
    }
    return null;
  }
}
