/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.kmeans;

import org.locationtech.geowave.analytic.AnalyticItemWrapper;
import org.locationtech.geowave.analytic.clustering.CentroidPairing;
import org.locationtech.geowave.analytic.distance.DistanceFn;

/**
 * Compute the distance of a points to the closest centroid, providing the resulting distance using
 * a provided distance function.
 */
public class CentroidAssociationFn<T> {
  private DistanceFn<T> distanceFunction;

  public DistanceFn<T> getDistanceFunction() {
    return distanceFunction;
  }

  public void setDistanceFunction(final DistanceFn<T> distanceFunction) {
    this.distanceFunction = distanceFunction;
  }

  public double compute(
      final AnalyticItemWrapper<T> point,
      final Iterable<AnalyticItemWrapper<T>> targetSet,
      final AssociationNotification<T> associationNotification) {
    final CentroidPairing<T> pairing = new CentroidPairing<>(null, point, Double.POSITIVE_INFINITY);
    for (final AnalyticItemWrapper<T> y : targetSet) {
      final double distance = distanceFunction.measure(point.getWrappedItem(), y.getWrappedItem());
      if (distance < pairing.getDistance()) {
        pairing.setDistance(distance);
        pairing.setCentroid(y);
      }
    }
    associationNotification.notify(pairing);
    return pairing.getDistance();
  }

  public double compute(
      final Iterable<AnalyticItemWrapper<T>> pointSet,
      final Iterable<AnalyticItemWrapper<T>> targetSet,
      final AssociationNotification<T> associationNotification) {
    double sum = 0.0;
    for (final AnalyticItemWrapper<T> point : pointSet) {
      sum += this.compute(point, targetSet, associationNotification);
    }
    return sum;
  }
}
