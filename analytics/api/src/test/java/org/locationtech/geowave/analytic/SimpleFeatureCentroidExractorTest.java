/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic;

import static org.junit.Assert.assertEquals;
import java.util.List;
import java.util.UUID;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Test;
import org.locationtech.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

public class SimpleFeatureCentroidExractorTest {

  SimpleFeatureCentroidExtractor extractor = new SimpleFeatureCentroidExtractor();

  @Test
  public void test() throws SchemaException {
    final SimpleFeatureType schema =
        DataUtilities.createType("testGeo", "location:Point:srid=4326,name:String");
    final List<AttributeDescriptor> descriptors = schema.getAttributeDescriptors();
    final Object[] defaults = new Object[descriptors.size()];
    int p = 0;
    for (final AttributeDescriptor descriptor : descriptors) {
      defaults[p++] = descriptor.getDefaultValue();
    }

    final SimpleFeature feature =
        SimpleFeatureBuilder.build(schema, defaults, UUID.randomUUID().toString());
    final GeometryFactory geoFactory = new GeometryFactory();

    feature.setAttribute("location", geoFactory.createPoint(new Coordinate(-45, 45)));

    final Point point = extractor.getCentroid(feature);
    assertEquals(4326, point.getSRID());
  }
}
