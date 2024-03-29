/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.gdelt;

import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.net.URL;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.ingest.DataSchemaOptionProvider;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.opengis.feature.simple.SimpleFeature;

public class GDELTIngestTest {
  private DataSchemaOptionProvider optionsProvider;
  private GDELTIngestPlugin ingester;
  private GDELTIngestPlugin ingesterExt;
  private String filePath;
  private int expectedCount;

  @Before
  public void setup() {
    optionsProvider = new DataSchemaOptionProvider();
    optionsProvider.setSupplementalFields(true);

    ingester = new GDELTIngestPlugin();
    ingester.init(null);

    ingesterExt = new GDELTIngestPlugin(optionsProvider);
    ingesterExt.init(null);

    filePath = "20130401.export.CSV.zip";
    expectedCount = 14056;
  }

  @Test
  public void testIngest() throws IOException {

    final URL toIngest = this.getClass().getClassLoader().getResource(filePath);

    assertTrue(GDELTUtils.validate(toIngest));
    final CloseableIterator<GeoWaveData<SimpleFeature>> features =
        ingester.toGeoWaveData(toIngest, new String[] {"123"});

    assertTrue((features != null) && features.hasNext());

    int featureCount = 0;
    while (features.hasNext()) {
      final GeoWaveData<SimpleFeature> feature = features.next();

      if (isValidGDELTFeature(feature)) {
        featureCount++;
      }
    }
    features.close();

    final CloseableIterator<GeoWaveData<SimpleFeature>> featuresExt =
        ingesterExt.toGeoWaveData(toIngest, new String[] {"123"});

    assertTrue((featuresExt != null) && featuresExt.hasNext());

    int featureCountExt = 0;
    while (featuresExt.hasNext()) {
      final GeoWaveData<SimpleFeature> featureExt = featuresExt.next();

      if (isValidGDELTFeatureExt(featureExt)) {
        featureCountExt++;
      }
    }
    featuresExt.close();

    final boolean readExpectedCount = (featureCount == expectedCount);
    if (!readExpectedCount) {
      System.out.println("Expected " + expectedCount + " features, ingested " + featureCount);
    }

    final boolean readExpectedCountExt = (featureCountExt == expectedCount);
    if (!readExpectedCount) {
      System.out.println("Expected " + expectedCount + " features, ingested " + featureCountExt);
    }

    assertTrue(readExpectedCount);
    assertTrue(readExpectedCountExt);
  }

  private boolean isValidGDELTFeature(final GeoWaveData<SimpleFeature> feature) {
    if ((feature.getValue().getAttribute(GDELTUtils.GDELT_EVENT_ID_ATTRIBUTE) == null)
        || (feature.getValue().getAttribute(GDELTUtils.GDELT_GEOMETRY_ATTRIBUTE) == null)
        || (feature.getValue().getAttribute(GDELTUtils.GDELT_LATITUDE_ATTRIBUTE) == null)
        || (feature.getValue().getAttribute(GDELTUtils.GDELT_LONGITUDE_ATTRIBUTE) == null)
        || (feature.getValue().getAttribute(GDELTUtils.GDELT_TIMESTAMP_ATTRIBUTE) == null)) {
      return false;
    }
    return true;
  }

  private boolean isValidGDELTFeatureExt(final GeoWaveData<SimpleFeature> featureExt) {
    if ((featureExt.getValue().getAttribute(GDELTUtils.GDELT_EVENT_ID_ATTRIBUTE) == null)
        || (featureExt.getValue().getAttribute(GDELTUtils.GDELT_GEOMETRY_ATTRIBUTE) == null)
        || (featureExt.getValue().getAttribute(GDELTUtils.GDELT_LATITUDE_ATTRIBUTE) == null)
        || (featureExt.getValue().getAttribute(GDELTUtils.GDELT_LONGITUDE_ATTRIBUTE) == null)
        || (featureExt.getValue().getAttribute(GDELTUtils.GDELT_TIMESTAMP_ATTRIBUTE) == null)
        || (featureExt.getValue().getAttribute(GDELTUtils.AVG_TONE_ATTRIBUTE) == null)) {
      return false;
    }
    return true;
  }
}
