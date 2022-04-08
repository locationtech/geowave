/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.clustering;

import org.locationtech.geowave.analytic.AnalyticItemWrapper;

public class CentroidPairing<T> {
  private AnalyticItemWrapper<T> centroid;
  private AnalyticItemWrapper<T> pairedItem;
  private double distance;

  public CentroidPairing() {}

  public CentroidPairing(
      final AnalyticItemWrapper<T> centroid,
      final AnalyticItemWrapper<T> pairedItem,
      final double distance) {
    super();
    this.centroid = centroid;
    this.pairedItem = pairedItem;
    this.distance = distance;
  }

  public AnalyticItemWrapper<T> getCentroid() {
    return centroid;
  }

  public void setCentroid(final AnalyticItemWrapper<T> centroid) {
    this.centroid = centroid;
  }

  public AnalyticItemWrapper<T> getPairedItem() {
    return pairedItem;
  }

  public void setPairedItem(final AnalyticItemWrapper<T> pairedItem) {
    this.pairedItem = pairedItem;
  }

  public double getDistance() {
    return distance;
  }

  public void setDistance(final double distance) {
    this.distance = distance;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((centroid == null) ? 0 : centroid.hashCode());
    long temp;
    temp = Double.doubleToLongBits(distance);
    result = (prime * result) + (int) (temp ^ (temp >>> 32));
    result = (prime * result) + ((pairedItem == null) ? 0 : pairedItem.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final CentroidPairing other = (CentroidPairing) obj;
    if (centroid == null) {
      if (other.centroid != null) {
        return false;
      }
    } else if (!centroid.equals(other.centroid)) {
      return false;
    }
    if (Double.doubleToLongBits(distance) != Double.doubleToLongBits(other.distance)) {
      return false;
    }
    if (pairedItem == null) {
      if (other.pairedItem != null) {
        return false;
      }
    } else if (!pairedItem.equals(other.pairedItem)) {
      return false;
    }
    return true;
  }
}
