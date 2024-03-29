/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.dbscan;

import org.locationtech.geowave.analytic.distance.CoordinateCircleDistanceFn;
import org.locationtech.geowave.analytic.distance.DistanceFn;
import org.locationtech.geowave.analytic.mapreduce.dbscan.ClusterItemDistanceFn.ClusterProfileContext;
import org.locationtech.geowave.analytic.nn.DistanceProfile;
import org.locationtech.geowave.analytic.nn.DistanceProfileGenerateFn;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.operation.distance.DistanceOp;

/** Calculate distance between two cluster items. */
public class ClusterItemDistanceFn implements
    DistanceFn<ClusterItem>,
    DistanceProfileGenerateFn<ClusterProfileContext, ClusterItem> {

  /** */
  private static final long serialVersionUID = 3824608959408031752L;

  private DistanceFn<Coordinate> coordinateDistanceFunction = new CoordinateCircleDistanceFn();

  /** Used to reduce memory GC */
  private static final ThreadLocal<DistanceProfile<ClusterProfileContext>> profile =
      new ThreadLocal<DistanceProfile<ClusterProfileContext>>() {
        @Override
        protected DistanceProfile<ClusterProfileContext> initialValue() {
          return new DistanceProfile<>(0.0, new ClusterProfileContext());
        }
      };

  public ClusterItemDistanceFn() {}

  public ClusterItemDistanceFn(final DistanceFn<Coordinate> coordinateDistanceFunction) {
    super();
    this.coordinateDistanceFunction = coordinateDistanceFunction;
  }

  public DistanceFn<Coordinate> getCoordinateDistanceFunction() {
    return coordinateDistanceFunction;
  }

  public void setCoordinateDistanceFunction(
      final DistanceFn<Coordinate> coordinateDistanceFunction) {
    this.coordinateDistanceFunction = coordinateDistanceFunction;
  }

  @Override
  public double measure(final ClusterItem x, final ClusterItem y) {

    final Geometry gx = x.getGeometry();
    final Geometry gy = y.getGeometry();
    if ((gx instanceof Point) && (gy instanceof Point)) {
      return coordinateDistanceFunction.measure(gx.getCoordinate(), gy.getCoordinate());
    }
    final DistanceOp op = new DistanceOp(gx, gy);
    final Coordinate[] points = op.nearestPoints();
    return coordinateDistanceFunction.measure(points[0], points[1]);
  }

  @Override
  public DistanceProfile<ClusterProfileContext> computeProfile(
      final ClusterItem item1,
      final ClusterItem item2) {
    final DistanceProfile<ClusterProfileContext> localProfile = profile.get();
    final ClusterProfileContext context = localProfile.getContext();
    final Geometry gx = item1.getGeometry();
    final Geometry gy = item2.getGeometry();
    context.setItem1(item1);
    context.setItem2(item2);
    if ((gx instanceof Point) && (gy instanceof Point)) {
      context.setPoint1(gx.getCoordinate());
      context.setPoint2(gy.getCoordinate());
    } else {
      final DistanceOp op = new DistanceOp(gx, gy);
      final Coordinate[] points = op.nearestPoints();
      context.setPoint1(points[0]);
      context.setPoint2(points[1]);
    }
    localProfile.setDistance(
        coordinateDistanceFunction.measure(context.getPoint1(), context.getPoint2()));
    return localProfile;
  }

  public static class ClusterProfileContext {
    private Coordinate point1;
    private ClusterItem item1;
    private Coordinate point2;
    private ClusterItem item2;

    public Coordinate getPoint1() {
      return point1;
    }

    public void setPoint1(final Coordinate point1) {
      this.point1 = point1;
    }

    public ClusterItem getItem1() {
      return item1;
    }

    public void setItem1(final ClusterItem item1) {
      this.item1 = item1;
    }

    public Coordinate getPoint2() {
      return point2;
    }

    public void setPoint2(final Coordinate point2) {
      this.point2 = point2;
    }

    public ClusterItem getItem2() {
      return item2;
    }

    public void setItem2(final ClusterItem item2) {
      this.item2 = item2;
    }
  }
}
