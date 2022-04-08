/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.landsat8;

import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.Every.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.Envelope2D;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Ignore;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;

@Ignore
public class SceneFeatureIteratorTest {
  private Matcher<SimpleFeature> hasProperties() {
    return new BaseMatcher<SimpleFeature>() {
      @Override
      public boolean matches(final Object item) {
        final SimpleFeature feature = (SimpleFeature) item;

        return (feature.getProperty("productId") != null)
            && (feature.getProperty("acquisitionDate") != null)
            && (feature.getProperty("cloudCover") != null)
            && (feature.getProperty("processingLevel") != null)
            && (feature.getProperty("path") != null)
            && (feature.getProperty("row") != null)
            && (feature.getProperty("sceneDownloadUrl") != null);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(
            "feature should have properties {productId, acquisitionDate, cloudCover, processingLevel, path, row, sceneDownloadUrl}");
      }
    };
  }

  private Matcher<SimpleFeature> inBounds(final BoundingBox bounds) {
    return new BaseMatcher<SimpleFeature>() {
      @Override
      public boolean matches(final Object item) {
        final SimpleFeature feature = (SimpleFeature) item;
        return feature.getBounds().intersects(bounds);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText("feature should be in bounds " + bounds);
      }
    };
  }

  @Test
  public void testIterate() throws IOException, CQLException {
    final boolean onlyScenesSinceLastRun = false;
    final boolean useCachedScenes = true;
    final boolean nBestScenesByPathRow = false;
    final int nBestScenes = 1;
    final Filter cqlFilter = CQL.toFilter("BBOX(shape,-76.6,42.34,-76.4,42.54) and band='BQA'");
    final String workspaceDir = Tests.WORKSPACE_DIR;

    final List<SimpleFeature> features = new ArrayList<>();
    try (SceneFeatureIterator iterator =
        new SceneFeatureIterator(
            onlyScenesSinceLastRun,
            useCachedScenes,
            nBestScenesByPathRow,
            nBestScenes,
            cqlFilter,
            workspaceDir)) {
      while (iterator.hasNext()) {
        features.add(iterator.next());
      }
    }

    assertEquals(features.size(), 1);
    assertThat(
        features,
        everyItem(
            allOf(
                hasProperties(),
                inBounds(
                    new Envelope2D(
                        new DirectPosition2D(-76.6, 42.34),
                        new DirectPosition2D(-76.4, 42.54))))));
  }
}
