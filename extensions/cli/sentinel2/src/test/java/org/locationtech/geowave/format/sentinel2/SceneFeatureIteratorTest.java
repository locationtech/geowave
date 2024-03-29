/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.sentinel2;

import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.Every.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import java.io.IOException;
import java.net.MalformedURLException;
import java.security.GeneralSecurityException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.Envelope2D;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.util.DateUtilities;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;

public class SceneFeatureIteratorTest {
  private Matcher<SimpleFeature> hasProperties() {
    return new BaseMatcher<SimpleFeature>() {
      @Override
      public boolean matches(final Object item) {
        final SimpleFeature feature = (SimpleFeature) item;

        return (feature.getProperty("shape") != null)
            && (feature.getProperty("entityId") != null)
            && (feature.getProperty("provider") != null)
            && (feature.getProperty("location") != null)
            && (feature.getProperty("productIdentifier") != null)
            && (feature.getProperty("productType") != null)
            && (feature.getProperty("collection") != null)
            && (feature.getProperty("platform") != null)
            && (feature.getProperty("processingLevel") != null)
            && (feature.getProperty("startDate") != null)
            && (feature.getProperty("quicklook") != null)
            && (feature.getProperty("thumbnail") != null)
            && (feature.getProperty("bands") != null)
            && (feature.getProperty("resolution") != null)
            && (feature.getProperty("cloudCover") != null)
            && (feature.getProperty("snowCover") != null)
            && (feature.getProperty("waterCover") != null);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(
            "feature should have properties {"
                + "shape, entityId, provider, location, productIdentifier, "
                + "productType, collection, platform, processingLevel, "
                + "startDate, quicklook, thumbnail, "
                + "bands, resolution, cloudCover, snowCover, waterCover"
                + "}");
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
  public void testIterateProviders()
      throws IOException, CQLException, ParseException, NoSuchAuthorityCodeException,
      FactoryException, MalformedURLException, GeneralSecurityException {
    for (final Sentinel2ImageryProvider provider : Sentinel2ImageryProvider.getProviders()) {
      testIterate(provider.providerName());
    }
  }

  public void testIterate(final String providerName)
      throws IOException, CQLException, ParseException, NoSuchAuthorityCodeException,
      FactoryException, MalformedURLException, GeneralSecurityException {

    final Sentinel2ImageryProvider provider = Sentinel2ImageryProvider.getProvider(providerName);
    if (provider == null) {
      throw new RuntimeException("Unable to find '" + providerName + "' Sentinel2 provider");
    }

    final String collection = provider.collections()[0];
    final String platform = "";
    final String location = "T30TWM";
    final Date startDate = DateUtilities.parseISO("2018-01-28T00:00:00Z");
    final Date endDate = DateUtilities.parseISO("2018-01-30T00:00:00Z");
    final int orbitNumber = 0;
    final int relativeOrbitNumber = 0;
    final Filter cqlFilter = CQL.toFilter("BBOX(shape,-1.8274,42.3253,-1.6256,42.4735)");
    final String workspaceDir = Tests.WORKSPACE_DIR;

    final List<SimpleFeature> features = new ArrayList<>();
    try (SceneFeatureIterator iterator =
        new SceneFeatureIterator(
            providerName,
            collection,
            platform,
            location,
            startDate,
            endDate,
            orbitNumber,
            relativeOrbitNumber,
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
                        new DirectPosition2D(-1.828, 42.325),
                        new DirectPosition2D(-1.624, 42.474))))));
  }
}
