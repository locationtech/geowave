/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.secondary;

import java.io.File;
import java.net.URL;
import java.util.function.BiConsumer;
import org.junit.Assert;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.opengis.feature.simple.SimpleFeature;
import com.aol.cyclops.util.function.TriConsumer;

abstract public class AbstractSecondaryIndexIT extends AbstractGeoWaveBasicVectorIT {
  protected void testIngestAndQuery(
      final DimensionalityType dimensionality,
      final BiConsumer<DimensionalityType, String> ingestFunction,
      final TriConsumer<URL, URL[], String> queryFunction,
      final BiConsumer<DimensionalityType, URL[]> verifyStats) throws Exception {
    ingestFunction.accept(dimensionality, HAIL_SHAPEFILE_FILE);
    ingestFunction.accept(dimensionality, TORNADO_TRACKS_SHAPEFILE_FILE);
    queryFunction.accept(
        new File(TEST_BOX_FILTER_FILE).toURI().toURL(),
        new URL[] {
            new File(HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL(),
            new File(TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL()},
        "bounding box");
    queryFunction.accept(
        new File(TEST_POLYGON_FILTER_FILE).toURI().toURL(),
        new URL[] {
            new File(HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL(),
            new File(TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL()},
        "polygon constraint");
    queryFunction.accept(
        new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
        new URL[] {
            new File(HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
            new File(TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()},
        "bounding box and time range");
    queryFunction.accept(
        new File(TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(),
        new URL[] {
            new File(HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
            new File(TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()},
        "polygon constraint and time range");
    final URL[] urls =
        new URL[] {
            new File(HAIL_SHAPEFILE_FILE).toURI().toURL(),
            new File(TORNADO_TRACKS_SHAPEFILE_FILE).toURI().toURL()};
    verifyStats.accept(dimensionality, urls);
    testQueryByDataId();
    testSpatialTemporalLocalExportAndReingestWithCQL(
        new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
        1,
        false,
        dimensionality);
    testDeleteDataId(
        new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
        dimensionality.getDefaultIndices()[0]);
    testDeleteCQL(CQL_DELETE_STR, null);

    testDeleteByBasicQuery(new File(TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(), null);
    testDeleteByBasicQuery(new File(TEST_POLYGON_FILTER_FILE).toURI().toURL(), null);
    TestUtils.deleteAll(getDataStorePluginOptions());
  }

  protected void testQueryByDataId() {
    VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
    try (CloseableIterator<SimpleFeature> it =
        getDataStorePluginOptions().createDataStore().query(
            bldr.constraints(
                bldr.constraintsFactory().dataIds(
                    StringUtils.stringToBinary("hail.860"))).build())) {
      // try(CloseableIterator<SimpleFeature> it =
      // getDataStorePluginOptions().createDataStore().query(bldr.build())){
      while (it.hasNext()) {
        final String id = it.next().getID();
        Assert.assertEquals("hail.860", id);
      }
    }
    bldr = VectorQueryBuilder.newBuilder();
    try (CloseableIterator<SimpleFeature> it =
        getDataStorePluginOptions().createDataStore().query(
            bldr.constraints(
                bldr.constraintsFactory().dataIdsByRange(
                    StringUtils.stringToBinary("hail.860"),
                    StringUtils.stringToBinary("hail.870"))).build())) {

      int i = 860;
      while (it.hasNext() && (i <= 870)) {
        final String id = it.next().getID();
        Assert.assertEquals("hail." + i++, id);
      }
      Assert.assertFalse("The iterator should be exhausted after hail.870", it.hasNext());
    }
  }
}
