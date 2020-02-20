/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics;

import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsType;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.operation.MathTransform;

public class FeatureBoundingBoxStatistics extends
    BoundingBoxDataStatistics<SimpleFeature, FieldStatisticsQueryBuilder<Envelope>> implements
    FieldNameStatistic {
  public static final FieldStatisticsType<Envelope> STATS_TYPE =
      new FieldStatisticsType<>("BOUNDING_BOX");
  private SimpleFeatureType reprojectedType;
  private MathTransform transform;

  public FeatureBoundingBoxStatistics() {
    super(STATS_TYPE);
  }

  public FeatureBoundingBoxStatistics(final String fieldName) {
    this(null, fieldName);
  }

  public FeatureBoundingBoxStatistics(final Short adapterId, final String fieldName) {
    this(adapterId, fieldName, null, null);
  }

  public FeatureBoundingBoxStatistics(
      final String fieldName,
      final SimpleFeatureType reprojectedType,
      final MathTransform transform) {
    this(null, fieldName, reprojectedType, transform);
  }

  public FeatureBoundingBoxStatistics(
      final Short adapterId,
      final String fieldName,
      final SimpleFeatureType reprojectedType,
      final MathTransform transform) {
    super(adapterId, STATS_TYPE, fieldName);
    this.reprojectedType = reprojectedType;
    this.transform = transform;
  }

  @Override
  public String getFieldName() {
    return extendedId;
  }

  @Override
  protected Envelope getEnvelope(final SimpleFeature entry) {
    // incorporate the bounding box of the entry's envelope
    final Object o;
    if ((reprojectedType != null)
        && (transform != null)
        && !reprojectedType.getCoordinateReferenceSystem().equals(
            entry.getType().getCoordinateReferenceSystem())) {
      o =
          GeometryUtils.crsTransform(entry, reprojectedType, transform).getAttribute(
              getFieldName());
    } else {
      o = entry.getAttribute(getFieldName());
    }
    if ((o != null) && (o instanceof Geometry)) {
      final Geometry geometry = (Geometry) o;
      if (!geometry.isEmpty()) {
        return geometry.getEnvelopeInternal();
      }
    }
    return null;
  }

  @Override
  public InternalDataStatistics<SimpleFeature, Envelope, FieldStatisticsQueryBuilder<Envelope>> duplicate() {
    return new FeatureBoundingBoxStatistics(adapterId, getFieldName(), reprojectedType, transform);
  }
}
