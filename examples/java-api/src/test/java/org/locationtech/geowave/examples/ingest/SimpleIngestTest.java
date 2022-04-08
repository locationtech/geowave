/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.ingest;

import java.util.Set;
import java.util.TreeSet;
import org.junit.Assert;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleIngestTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleIngestTest.class);

  final GeometryFactory factory = new GeometryFactory();
  IndexStore indexStore;
  PersistentAdapterStore adapterStore;
  DataStatisticsStore statsStore;

  protected static Set<Point> getCalcedPointSet() {
    final Set<Point> calcPoints = new TreeSet<>();
    for (int longitude = -180; longitude <= 180; longitude += 5) {
      for (int latitude = -90; latitude <= 90; latitude += 5) {
        final Point p =
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude));
        calcPoints.add(p);
      }
    }
    return calcPoints;
  }

  protected static Set<Point> getStoredPointSet(final DataStore ds) {
    final CloseableIterator itr =
        ds.query(
            QueryBuilder.newBuilder().constraints(
                new BasicQueryByClass(new BasicQueryByClass.ConstraintsByClass())).build());
    final Set<Point> readPoints = new TreeSet<>();
    while (itr.hasNext()) {
      final Object n = itr.next();
      if (n instanceof SimpleFeature) {
        final SimpleFeature gridCell = (SimpleFeature) n;
        final Point p = (Point) gridCell.getDefaultGeometry();
        readPoints.add(p);
      }
    }
    return readPoints;
  }

  protected static void validate(final DataStore ds) {
    final Set<Point> readPoints = getStoredPointSet(ds);
    final Set<Point> calcPoints = getCalcedPointSet();

    Assert.assertTrue(readPoints.equals(calcPoints));
  }
}
