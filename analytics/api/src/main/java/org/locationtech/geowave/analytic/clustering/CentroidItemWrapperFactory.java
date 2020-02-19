/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.clustering;

import java.io.IOException;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.analytic.AnalyticItemWrapper;
import org.locationtech.geowave.analytic.AnalyticItemWrapperFactory;
import org.locationtech.geowave.analytic.kmeans.AssociationNotification;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determine the group ID for an item dynamically.
 *
 * @param <T>
 */
public class CentroidItemWrapperFactory<T> implements AnalyticItemWrapperFactory<T> {

  static final Logger LOGGER = LoggerFactory.getLogger(CentroidItemWrapperFactory.class);
  private AnalyticItemWrapperFactory<T> itemFactory;
  private NestedGroupCentroidAssignment<T> nestedGroupCentroidAssignment;

  @Override
  public AnalyticItemWrapper<T> create(final T item) {
    return new CentroidItemWrapper(item);
  }

  @Override
  public void initialize(final JobContext context, final Class<?> scope, final Logger logger)
      throws IOException {
    try {
      nestedGroupCentroidAssignment = new NestedGroupCentroidAssignment<>(context, scope, logger);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IOException("Failed to instantiate", e);
    }

    itemFactory.initialize(context, scope, logger);
  }

  public AnalyticItemWrapperFactory<T> getItemFactory() {
    return itemFactory;
  }

  public void setItemFactory(final AnalyticItemWrapperFactory<T> itemFactory) {
    this.itemFactory = itemFactory;
  }

  public class CentroidItemWrapper implements AnalyticItemWrapper<T> {
    final AnalyticItemWrapper<T> wrappedItem;
    AnalyticItemWrapper<T> centroidItem;

    public CentroidItemWrapper(final T item) {
      wrappedItem = itemFactory.create(item);
      try {
        nestedGroupCentroidAssignment.findCentroidForLevel(
            wrappedItem,
            new AssociationNotification<T>() {
              @Override
              public void notify(final CentroidPairing<T> pairing) {
                centroidItem = pairing.getCentroid();
              }
            });
      } catch (final IOException e) {
        LOGGER.error("Cannot resolve paired centroid for " + wrappedItem.getID(), e);
        centroidItem = wrappedItem;
      }
    }

    @Override
    public String getID() {
      return centroidItem.getID();
    }

    @Override
    public T getWrappedItem() {
      return centroidItem.getWrappedItem();
    }

    @Override
    public long getAssociationCount() {
      return centroidItem.getAssociationCount();
    }

    @Override
    public int getIterationID() {
      return centroidItem.getIterationID();
    }

    // this is not a mistake...the group id is the centroid itself
    @Override
    public String getGroupID() {
      return centroidItem.getID();
    }

    @Override
    public void setGroupID(final String groupID) {}

    @Override
    public void resetAssociatonCount() {}

    @Override
    public void incrementAssociationCount(final long increment) {}

    @Override
    public double getCost() {
      return centroidItem.getCost();
    }

    @Override
    public void setCost(final double cost) {}

    @Override
    public String getName() {
      return centroidItem.getName();
    }

    @Override
    public String[] getExtraDimensions() {
      return new String[0];
    }

    @Override
    public double[] getDimensionValues() {
      return new double[0];
    }

    @Override
    public Geometry getGeometry() {
      return centroidItem.getGeometry();
    }

    @Override
    public void setZoomLevel(final int level) {}

    @Override
    public int getZoomLevel() {
      return centroidItem.getZoomLevel();
    }

    @Override
    public void setBatchID(final String batchID) {}

    @Override
    public String getBatchID() {
      return centroidItem.getBatchID();
    }
  }

  /*
   * @see org.locationtech.geowave.analytics.tools.CentroidFactory#createNextCentroid
   * (java.lang.Object, org.locationtech.jts.geom.Coordinate, java.lang.String[], double[])
   */

  @Override
  public AnalyticItemWrapper<T> createNextItem(
      final T feature,
      final String groupID,
      final Coordinate coordinate,
      final String[] extraNames,
      final double[] extraValues) {
    return this.itemFactory.createNextItem(feature, groupID, coordinate, extraNames, extraValues);
  }
}
